from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from datetime import datetime, timedelta
import boto3
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import time
from airflow.models import Variable



client = boto3.client('glue')

##########################################################
# CONSTANTS AND GLOBAL VARIABLES DEFINITION
##########################################################
default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 1, 11),   
    'retries': 2,
}




##########################################################
# DAG DEFINITIONS
##########################################################
dag = DAG(dag_id= 'karpenter_pipeline',
          schedule_interval= None,
          catchup=False,
          default_args = default_args
          )


##########################################################
# DUMMY OPERATORS
##########################################################
# task order: first
start_flow = DummyOperator(
    task_id='start_flow',
    trigger_rule="dummy",
    dag=dag)

p_flow_1 = DummyOperator(
    task_id='p_flow_1',
    trigger_rule="dummy",
    dag=dag)

p_flow_2 = DummyOperator(
    task_id='p_flow_2',
    trigger_rule="dummy",
    dag=dag)

p_flow_3 = DummyOperator(
    task_id='p_flow_3',
    trigger_rule="dummy",
    dag=dag)

p_flow_4 = DummyOperator(
    task_id='p_flow_4',
    trigger_rule="dummy",
    dag=dag)

p_flow_5 = DummyOperator(
    task_id='p_flow_5',
    trigger_rule="dummy",
    dag=dag)

p_flow_6 = DummyOperator(
    task_id='p_flow_6',
    trigger_rule="dummy",
    dag=dag)

# task order: last
end_flow = DummyOperator(
    task_id='end_flow',
    dag=dag)

delay_python_task1: PythonOperator = PythonOperator(task_id="delay_python_task1",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(300))

delay_python_task2: PythonOperator = PythonOperator(task_id="delay_python_task2",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(200))

delay_python_task3: PythonOperator = PythonOperator(task_id="delay_python_task3",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(300))


start_flow >>[delay_python_task1, delay_python_task2, delay_python_task3, p_flow_1, p_flow_2, p_flow_3, p_flow_4, p_flow_5, p_flow_6]>> end_flow