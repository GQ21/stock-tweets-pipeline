from airflow.models import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from datetime import datetime
import config.job_overrides as jo
import config.spark_steps as ss


s3_emr_path = "s3://stwit-jobs/emr/"

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
    job_flow_overrides=jo.JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=ss.SPARK_STEPS, 
    params={"s3_emr_path": s3_emr_path},  
    dag=dag,
)

last_step = len(ss.SPARK_STEPS) - 1
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

