from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteBatchOperator
import datetime

PROJECT_ID = "haripro-439108"
CLUSTER_NAME="singlenode-dpeph-cluster"
REGION = "us-central1"
ZONE = "us-central1-a"
PYSPARK_CODE1_URI = "gs://we43-learn1-hari/code/code_Usecase6_step1_gcs_bq.py"
BIGQUERY_CONNECTOR_JAR="gs://spark-lib/bigquery/spark-3.1-bigquery-0.32.2.jar"

# Define default arguments
default_args = {
    'owner': 'airflow',
    "start_date": days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1}

# Define the DAG
dag = DAG(
    'DAG-Usecase-8-To-Submit-PySpark-Task-Serverless-Spark-Cluster',
    default_args=default_args,
    description='Submit a serverless Spark job to Dataproc',
    schedule_interval=datetime.timedelta(days=1),  # Set to your desired schedule
    tags=['wd32-serverless'],
)

# Define the configuration for the Dataproc batch job
batch_config = {
    "pyspark_batch": {
        "main_python_file_uri": PYSPARK_CODE1_URI,
		"jar_file_uris": [BIGQUERY_CONNECTOR_JAR]
    },
    "runtime_config": {
        "version": "1.1",
        "properties": {
            "spark.executor.cores": "4",
            "spark.driver.cores": "4",
            "spark.executor.instances": "2",
        },
    },
    "environment_config": {
        "execution_config": {
            "subnetwork_uri": "default",
        },
    },
    "labels": {
        "label": "iz_spark_serverless",
    },
}

# Define the task to create the Dataproc batch job
create_batch = DataprocCreateBatchOperator(
    task_id='create_batch',
    project_id=PROJECT_ID,
    region=REGION,
    batch=batch_config,
    batch_id='iz-serveless-spark-batch3',
    dag=dag,
)

# Define the task to delete the Dataproc batch job after completion
delete_batch = DataprocDeleteBatchOperator(
    task_id='delete_batch',
    project_id=PROJECT_ID,
    region=REGION,
    batch_id='iz-serveless-spark-batch3',
    trigger_rule='all_done',
    dag=dag,
)

# Set task dependencies
create_batch >> delete_batch