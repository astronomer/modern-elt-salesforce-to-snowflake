"""
### ELT with Snowflake and Salesforce

DAG that showcases modern ELT pattern by moving data from Salesforce into Snowflake. 
This DAG uses a custom built Salesforce to S3 operator, not an integration service like Fivetran or Airbyte.

After the data is inserted, a report is refreshed with the new data. 
"""

from datetime import datetime

from airflow.models import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.transfers.salesforce_to_s3 import SalesforceToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.utils.trigger_rule import TriggerRule


DATA_LAKE_LANDING_BUCKET = "{{ var.json.data_lake_info.data_lake_landing_bucket }}"
DATA_LAKE_RAW_BUCKET = "{{ var.json.data_lake_info.data_lake_raw_bucket }}"
DATE_FOLDER_PATH = "{{ execution_date.strftime('%Y/%m/%d') }}"
SALESFORCE_S3_BASE_PATH = "salesforce/accounts"
SALESFORCE_FILE_NAME = "accounts_extract_{{ ds_nodash }}.csv"

AWS_CONN_ID = "s3"
SNOWFLAKE_CONN_ID = "snowflake"


with DAG(
    dag_id="modern_elt",
    start_date=datetime(2021, 6, 29),
    schedule_interval="@daily",
    catchup=False,
    default_args={"retries": 1},
    template_searchpath="include/sql",
    default_view="graph",
) as dag:
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    # Query Salesforce to extract Account data and store in an S3 data lake landing area.
    upload_salesforce_data_to_s3_landing = SalesforceToS3Operator(
        task_id="upload_salesforce_data_to_s3_landing",
        salesforce_query="salesforce/extract/extract_accounts.sql",
        s3_bucket_name=DATA_LAKE_LANDING_BUCKET,
        s3_key=f"{SALESFORCE_S3_BASE_PATH}/{SALESFORCE_FILE_NAME}",
        salesforce_conn_id="salesforce",
        aws_conn_id=AWS_CONN_ID,
        replace=True,
    )

    # Truncate stage for Customers (aka Accounts in Salesforce) data in Snowflake prior to load.
    truncate_snowflake_stage_table = SnowflakeOperator(
        task_id="truncate_snowflake_stage_table",
        sql="snowflake/common/truncate_table.sql",
        params={"table_name": "customers_staging"},
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # Copy the landed files from S3 to the Snowflake stage table.
    copy_from_s3_to_snowflake = S3ToSnowflakeOperator(
        task_id="copy_from_s3_to_snowflake",
        stage="s3_elt_data_lake_landing",
        prefix=SALESFORCE_S3_BASE_PATH,
        file_format="S3_LANDING_CSV",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        table="customers_staging",
    )

    # Insert into the dimensional Customer table in Snowflake.
    load_snowflake_staging_data = SnowflakeOperator(
        task_id="load_snowflake_staging_data",
        sql="snowflake/staging/load_customers_staging.sql",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # Rebuild website-traffic reporting tables after new Customers are inserted.
    refresh_reporting_table = SnowflakeOperator(
        task_id="refresh_reporting_table",
        sql="snowflake/reporting/build_registry_reporting.sql",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # Move the landed Accounts data file from the S3 landing area to the persistent data lake area.
    store_to_s3_data_lake = S3CopyObjectOperator(
        task_id="store_to_s3_data_lake",
        source_bucket_key=upload_salesforce_data_to_s3_landing.output,
        dest_bucket_name=DATA_LAKE_RAW_BUCKET,
        dest_bucket_key=f"{SALESFORCE_S3_BASE_PATH}/{DATE_FOLDER_PATH}/{SALESFORCE_FILE_NAME}",
        aws_conn_id=AWS_CONN_ID,
    )

    # Delete the landed Accounts data file after persisting to the data lake
    delete_data_from_s3_landing = S3DeleteObjectsOperator(
        task_id="delete_data_from_s3_landing",
        bucket=DATA_LAKE_LANDING_BUCKET,
        keys=f"{SALESFORCE_S3_BASE_PATH}/{SALESFORCE_FILE_NAME}",
        aws_conn_id=AWS_CONN_ID,
    )

    # Set task dependencies.
    chain(
        begin,
        upload_salesforce_data_to_s3_landing,
        truncate_snowflake_stage_table,
        copy_from_s3_to_snowflake,
        store_to_s3_data_lake,
        delete_data_from_s3_landing,
    )

    chain(copy_from_s3_to_snowflake, load_snowflake_staging_data, refresh_reporting_table)

    chain([delete_data_from_s3_landing, refresh_reporting_table], end)

    # Task dependency created by XComArgs
    #   upload_salesforce_data_to_s3_landing >> store_to_s3_data_lake
