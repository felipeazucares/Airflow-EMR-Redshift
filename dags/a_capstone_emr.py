from datetime import datetime, timedelta
import logging
import os
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, \
    IntegerType as Int, LongType as Lng, TimestampType as Tms, DateType as Dt, FloatType as Ft
from functools import reduce
from pyspark.sql import DataFrame


def stub():
    # this doesn't do anything
    logging.info("Dummy function called")


# configuration information
BUCKET_NAME = "capstone-suggars"
#local_data = "./dags/data/movie_review.csv"
s3_data_bucket = "data2/"
s3_analytics_bucket = "analytics/"
s3_script = "process_i94.py"

# define tbhe EMR instance details

# Boto3 job flow parameters see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.run_job_flow
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
                "InstanceCount": 2,
            },
        ],
        # if we add a key in then we can ssh to this instance post launch - this is the name of the EC2 key in the EC2 dashboard>>key pair
        "Ec2KeyName": "EMR_KEY",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        # as above speciffy specific sec groups to enable access to debug jobs
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
                "--src=s3a://{{ params.BUCKET_NAME }}/{{ params.s3_data }}",
                "--dest=hdfs:///user/hadoop/i94",
            ],
        },
    },
    {
        "Name": "install pre-requisites",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "sudo pip install configparser",
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
                "s3a://{{ params.BUCKET_NAME }}/pyspark_steps/{{ params.s3_script }}",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=hdfs:///user/hadoop/analytics",
                "--dest=s3a://{{ params.BUCKET_NAME }}/{{ params.s3_output }}",
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("a_capstone_emr",
          default_args=default_args,
          description="Build fact and dimension table for capstone project on i94 arrivals data",
          schedule_interval="@monthly",
          catchup=True,
          )

start_operator = DummyOperator(task_id="Begin_execution",  dag=dag)


create_emr_instance = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={  # these params are used to fill the paramterized values in SPARK_STEPS json
        "BUCKET_NAME": BUCKET_NAME,
        "s3_data": s3_data_bucket,
        "s3_script": "process_state_dimension_data.py",
        "s3_output": s3_analytics_bucket,
    },
    dag=dag,
)

# this value will let the sensor know the last step to watch
last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# move_raw_data = DummyOperator(
#     task_id="move_raw_data",
#     dag=dag,
# )

# process_data = DummyOperator(
#     task_id="process_data",
#     dag=dag,
# )

# run_quality_checks = DummyOperator(
#     task_id="Run_data_quality_checks",
#     dag=dag,
# )

end_operator = DummyOperator(task_id="Stop_execution",  dag=dag)

start_operator >> create_emr_instance >> step_adder >> step_checker >> end_operator
