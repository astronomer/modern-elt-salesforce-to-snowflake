# Modern ELT: Salesforce to Snowflake
### An ELT data pipeline from extracting Salesforce data to loading and transforming in Snowflake.
</br>

The DAG is this repo demonstrates a use case in which an analyst needs to blend Salesforce CRM and webpage analytics data within their Snowflake data warehouse for reporting.  The data extracted from Salesforce is landed in an AWS S3 bucket for ingestion, copied directly from S3 to Snowflake, and finally transformed for analytics.  In this example, S3 is also used as a data lake so the landed Salesforce data is also then persisted in a "raw data" S3 bucket.

</br>

**Airflow Version**

   `2.1.0`

**Providers Used**

   ```
   apache-airflow-providers-amazon==2.1.0
   apache-airflow-providers-http==2.0.1
   apache-airflow-providers-salesforce==3.1.0
   apache-airflow-providers-snowflake==2.0.0
   ```

**Connections**
- Salesforce
- AWS S3
- Snowflake
