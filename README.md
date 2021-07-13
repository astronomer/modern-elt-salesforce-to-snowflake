# Modern ELT: Salesforce to Snowflake
### A modern ELT pipeline from extracting Salesforce data to loading and transforming in Snowflake.
</br>

The DAG is this repo demonstrates a use case in which an analyst needs to blend Salesforce CRM and webpage analytics data within their Snowflake data warehouse for reporting.  The data extracted from Salesforce is landed in an AWS S3 bucket for ingestion, copied directly from S3 to Snowflake, and finally transformed for analytics.  In this example, S3 is also used as a data lake so the landed Salesforce data is also then persisted in a "raw data" S3 bucket.

> **Note:** A custom [`SalesforceToS3Operator`](https://github.com/astronomer/modern-elt-salesforce-to-snowflake/blob/main/include/operators/salesforce_to_s3.py) was created for this DAG to extract Salesforce data via a SOSQL query and stored as a file in an S3 bucket -- soon to be a part of the Amazon AWS provider in Airflow.

</br>

**Airflow Version**

   `2.1.1`

**Providers**

   ```
   apache-airflow-providers-amazon==2.0.0
   apache-airflow-providers-salesforce==3.0.0
   apache-airflow-providers-snowflake==2.0.0
   ```
