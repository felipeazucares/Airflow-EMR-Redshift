# DAG for Data Engineering Capstone project
# Philip Suggars February 2021
# creates EMR + Hadoop cluster
# adds steps to:
#   1. Copy  datasets from s3 to hadoop
#   2. Process datasets - summarise i94 and temperature data by month and state write to fact table,
#      create state dimension table. Write outputs to parquet
#   3. Carry out DQ checks
#   4. Copy processed data back to S3
#   5. Shutdown EMR cluster
#   6. Creates dimension and fact tables in pre-existing redshift instance
#   7. populates tables from processed data in S3


from pyspark.sql import functions as F
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import StageToRedshiftOperator
from helpers import SqlQueries

# configuration information
BUCKET_NAME = "capstone-suggars"
S3_DATA_BUCKET = "data/"
S3_ANALYTICS_BUCKET = "analytics/"
S3_SCRIPT = "process_i94.py"
S3_SCRIPT_BUCKET = "pyspark_steps"
DIMENSION_STATE_KEY = "dimension_state"
FACT_ARRIVALS_TABLE = "fact_arrivals"
DIMENSION_STATE_TABLE = "dimension_state"
FACT_ARRIVALS_KEY = "fact_arrivals_by_state_month"


# define the EMR instance details

# Boto3 job flow parameters see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html
# EMR.Client.run_job_flow
JOB_FLOW_OVERRIDES = {
    "Name": "i94_capstone",
    "ReleaseLabel": "emr-5.29.0",
    # We want our EMR cluster to have HDFS and Spark
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 3,
            },
        ],
        # if we add a key in then we can ssh to this instance post launch - this is the name of the EC2 key in the
        # EC2 dashboard>>key pair
        "Ec2KeyName": "EMR_KEY",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        # as above specify specific sec groups to enable access to debug jobs
        "EmrManagedMasterSecurityGroup": "sg-019400a9e885f3e23",
        "EmrManagedSlaveSecurityGroup": "sg-019400a9e885f3e23",
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

SPARK_STEPS = [
    {
        "Name": "Move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3a://{{ params.bucket_name }}/{{ params.s3_data }}",
                "--dest=hdfs:///user/hadoop/i94",
            ],
        },
    },
    {
        "Name": "Build state dimension table",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3a://{{params.bucket_name}}/{{params.s3_script_bucket}}/process_state_dimension_data.py",
            ],
        },
    },
    {
        "Name": "Process temperature by state",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3a://{{ params.bucket_name }}/{{ params.s3_script_bucket }}/process_temperature_fact_data.py",
            ],
        },
    },
    {
        "Name": "Process i94 fact data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--packages",
                "saurfang:spark-sas7bdat:2.0.0-s_2.10",
                "s3a://{{ params.bucket_name }}/{{ params.s3_script_bucket }}/process_i94_fact_data.py",

            ],
        },
    },
    {
        "Name": "Data Quality Checks",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3a://{{ params.bucket_name }}/{{ params.s3_script_bucket }}/data_quality_checks.py",

            ],
        },
    },
    {
        "Name": "Move processed data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=hdfs:///user/hadoop/analytics",
                "--dest=s3a://{{ params.bucket_name }}/{{ params.s3_output }}",
            ],
        },
    },
]

default_args = {
    "owner": "philip suggars",
    "start_date": datetime(2021, 2, 10),
    "end_date": datetime(2021, 2, 28),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG("capstone_emr_redshift",
          default_args=default_args,
          description="Build fact and dimension table for capstone project on i94 arrivals data",
          schedule_interval="@monthly",
          catchup=True,
          )

start_operator = DummyOperator(task_id="Begin_execution",  dag=dag)

# Empty out the analytics bucket - otherwise we aggregate results from successive runs
bucket_name = BUCKET_NAME + "/" + S3_ANALYTICS_BUCKET
empty_bucket = BashOperator(
    task_id="empty_bucket",
    bash_command="aws s3 rm s3://{} --recursive".format(
        bucket_name),
    dag=dag,
)

# Create EMR instance
create_EMR_instance = EmrCreateJobFlowOperator(
    task_id="create_EMR_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag
)

# Add your steps to the EMR cluster
EMR_step_adder = EmrAddStepsOperator(
    task_id="EMR_step_adder",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_EMR_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={  # these params are used to provide the parameters for the steps JSON above
        "bucket_name": BUCKET_NAME,
        "s3_data": S3_DATA_BUCKET,
        "s3_script_bucket": S3_SCRIPT_BUCKET,
        "s3_output": S3_ANALYTICS_BUCKET,
    },
    dag=dag,
)


# get the number of the final step
final_EMR_step = len(SPARK_STEPS) - 1
# wait for the steps to complete - seem to have to use concatenation here as {}.format()
# seems to fail because of double {} in source string
EMR_step_checker = EmrStepSensor(
    task_id="EMR_step_checker",
    job_flow_id="{{ task_instance.xcom_pull('create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='EMR_step_adder', key='return_value')["
    + str(final_EMR_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# Shutdown EMR cluster
shutdown_EMR_cluster = EmrTerminateJobFlowOperator(
    task_id="shutdown_EMR_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_EMR_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)
# Now create dimension table
create_dimension_table = PostgresOperator(
    task_id="create_dimension_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_dimension_table
)
# and create fact table
create_fact_table = PostgresOperator(
    task_id="create_fact_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_fact_table
)

populate_dimension_table = StageToRedshiftOperator(
    task_id="populate_dimension_table",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table=DIMENSION_STATE_TABLE,
    s3_bucket=BUCKET_NAME+"/"+S3_ANALYTICS_BUCKET,
    s3_key=DIMENSION_STATE_KEY,
    context=True
)
populate_fact_table = StageToRedshiftOperator(
    task_id="populate_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table=FACT_ARRIVALS_TABLE,
    s3_bucket=BUCKET_NAME+"/"+S3_ANALYTICS_BUCKET,
    s3_key=FACT_ARRIVALS_KEY,
    context=True
)

end_operator = DummyOperator(task_id="Stop_execution",  dag=dag)

start_operator >> empty_bucket >> create_EMR_instance >> EMR_step_adder
EMR_step_adder >> EMR_step_checker >> shutdown_EMR_cluster
shutdown_EMR_cluster >> create_dimension_table >> create_fact_table >> populate_dimension_table
populate_dimension_table >> populate_fact_table >> end_operator
