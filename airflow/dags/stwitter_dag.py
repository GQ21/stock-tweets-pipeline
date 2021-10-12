from airflow.models import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from datetime import datetime
from airflow.utils.dates import days_ago

s3_emr_path = "s3://stwit-jobs/emr/"


JOB_FLOW_OVERRIDES = {
    "Name": "Stwitter Cluster",
    "ReleaseLabel": "emr-5.33.0",
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
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "BootstrapActions":[{'Name': 'Install libraries','ScriptBootstrapAction': {'Path': 's3://stwit-jobs/EMR-install-libraries.sh'}}],
    "LogUri": "s3://aws-logs-356383675845-eu-north-1/elasticmapreduce/",
}


SPARK_STEPS = [
    {
        "Name": "Convert JSON to CSV data",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "--py-files",
                "{{params.s3_emr_path}}stwitter_emr.zip",
                "{{params.s3_emr_path}}stwitter_convert.py"               
            ],
        },
    },
    {
        "Name": "Process data",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "--py-files",
                "{{params.s3_emr_path}}stwitter_emr.zip",
                "{{params.s3_emr_path}}stwitter_process.py"               
            ],
        },
    },
    {
        "Name": "Predict sentiments",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "--py-files",
                "{{params.s3_emr_path}}stwitter_emr.zip",
                "{{params.s3_emr_path}}stwitter_infer.py"               
            ],
        },
    },
    {
        "Name": "Data quality check",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "--py-files",
                "{{params.s3_emr_path}}stwitter_emr.zip",
                "{{params.s3_emr_path}}stwitter_quality.py"               
            ],
        },
    },
    {
        "Name": "Ingest data into staging schema",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "--py-files",
                "{{params.s3_emr_path}}stwitter_emr.zip",
                "{{params.s3_emr_path}}stwitter_stage.py"               
            ],
        },
    },
    {
        "Name": "Ingest data into analytic schema",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "--py-files",
                "{{params.s3_emr_path}}stwitter_emr.zip",
                "{{params.s3_emr_path}}stwitter_analytic.py"               
            ],
        },
    }       
]

default_args = {
    "owner": "airflow",   
    'start_date': datetime(year=2021, month=9, day=27)
}

dag = DAG(
    "module_test_dag",
    default_args=default_args,
    description='Get twitter json files from s3 landing bucket, process it and import into data warehouse', 
    schedule_interval='00 03 * * *',
    max_active_runs = 1 
)

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS, 
    params={"s3_emr_path": s3_emr_path},  
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["+ str(last_step)+ "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

create_emr_cluster >> step_adder >>  step_checker >> terminate_emr_cluster

