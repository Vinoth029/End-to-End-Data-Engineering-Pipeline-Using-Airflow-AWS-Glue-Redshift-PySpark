from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3PrefixSensor
from airflow.providers.amazon.aws.operators.glue import AwsGlueCrawlerOperator, AwsGlueJobOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import boto3, json

default_args = {
    'owner': 'vinoth',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='s3_to_redshift_scd2_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    wait_for_files = S3PrefixSensor(
        task_id='wait_for_files',
        bucket_name='ingest-bucket',
        prefix='incoming/dataset/{{ ds_nodash }}',
        poke_interval=60,
        timeout=21600
    )

    run_crawler = AwsGlueCrawlerOperator(
        task_id='run_glue_crawler',
        config={'Name': 'dataset-crawler'}
    )

    run_data_quality = AwsGlueJobOperator(
        task_id='run_data_quality_job',
        job_name='dataset_data_quality_job',
        script_args={
            '--s3_input': 's3://ingest-bucket/incoming/dataset/{{ ds_nodash }}',
            '--results_path': '{{ ds_nodash }}'
        }
    )

    def check_dq_results(**context):
        bucket='ingest-bucket'
        key=f'quality_results/{context["ds_nodash"]}/quality_report.json'
        s3 = boto3.client('s3')
        resp = s3.get_object(Bucket=bucket, Key=key)
        report = json.loads(resp['Body'].read())
        if not report.get("passed", False):
            raise Exception("Data quality failed")

    dq_validation = PythonOperator(
        task_id='dq_validation',
        python_callable=check_dq_results
    )

    run_transform = AwsGlueJobOperator(
        task_id='run_transform_job',
        job_name='dataset_transform_job',
        script_args={
            '--s3_input': 's3://ingest-bucket/incoming/mydataset/{{ ds_nodash }}',
            '--s3_output': 's3://processed-bucket/processed/mydataset/{{ ds_nodash }}'
        }
    )

    copy_to_redshift = PostgresOperator(
        task_id='copy_to_redshift',
        postgres_conn_id='redshift_conn',
        sql="""
        COPY staging_mydataset
        FROM 's3://processed-bucket/processed/dataset/{{ ds_nodash }}/'
        IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftCopyRole'
        FORMAT AS PARQUET;
        """
    )

    merge_scd2 = PostgresOperator(
        task_id='merge_scd2',
        postgres_conn_id='redshift_conn',
        sql='sql/merge_scd2.sql'
    )

    (
        wait_for_files
        >> run_crawler
        >> run_data_quality
        >> dq_validation
        >> run_transform
        >> copy_to_redshift
        >> merge_scd2
    )
