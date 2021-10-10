from airflow.models import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from datetime import datetime
from airflow.utils.dates import days_ago

jobs_bucket = "stwit-jobs"
s3_script_convert = "stwitter_convert_to_csv.py"
s3_script_process = "stwitter_process.py"
s3_script_inference = "stwitter_inference.py"
s3_script_quality = "stwitter_quality_check.py"
s3_script_ingest_staging = "stwitter_ingest_staging.py"
s3_script_ingest_analytic = "stwitter_ingest_analytic.py"
s3_emr_libraries = "EMR-install-libraries.sh"
s3_logs_path = "s3://aws-logs-356383675845-eu-north-1/elasticmapreduce/"

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
    "BootstrapActions":[{'Name': 'Install libraries','ScriptBootstrapAction': {'Path': f's3://{jobs_bucket}/{s3_emr_libraries}'}}],
    "LogUri": s3_logs_path,
}


SPARK_STEPS = [
    {
        "Name": "Convert JSON to CSV data",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{params.jobs_bucket}}/{{params.s3_script_convert}}",
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
                "--deploy-mode",
                "client",
                "s3://{{params.jobs_bucket}}/{{params.s3_script_process}}",
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
                "--deploy-mode",
                "client",
                "s3://{{params.jobs_bucket}}/{{params.s3_script_inference}}",
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
                "--deploy-mode",
                "client",
                "s3://{{params.jobs_bucket}}/{{params.s3_script_quality}}",
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
                "--deploy-mode",
                "client",
                "s3://{{params.jobs_bucket}}/{{params.s3_script_ingest_staging}}",
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
                "--deploy-mode",
                "client",
                "s3://{{params.jobs_bucket}}/{{params.s3_script_ingest_analytic}}",
            ],
        },
    }
]

default_args = {
    "owner": "airflow", 
    'start_date': datetime(year=2021, month=9, day=27)
    #'start_date': days_ago(1)
}

dag = DAG(
    "stwitter_dag",
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
    params={
        "jobs_bucket": jobs_bucket,    
        "s3_script_convert" : s3_script_convert,
        "s3_script_process" : s3_script_process,
        "s3_script_inference" : s3_script_inference,
        "s3_script_quality" : s3_script_quality,
        "s3_script_ingest_staging" : s3_script_ingest_staging,
        "s3_script_ingest_analytic" : s3_script_ingest_analytic,
    },
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

