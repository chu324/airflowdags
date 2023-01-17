from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.operators.dummy_operator import DummyOperator



# CONSTANTS AND GLOBAL VARIABLES DEFINITION
default_args = {
    'owner': 'qchu',
    'start_date': datetime(2023, 12, 1),   
    'retries': 0,
}


# DAG DEFINITIONS
dag = DAG(dag_id= 'pl_cbdt',
          schedule_interval= None,
          catchup=False,
          default_args = default_args
          )


# OPERATOR DEFINITIONS
start_flow = DummyOperator(
    task_id='start_flow',
    trigger_rule="dummy",
    dag=dag)

end_flow = DummyOperator(
    task_id='end_flow',
    dag=dag)

cbdt_sftpToRaw = AwsGlueJobOperator(
    job_name: str = "cbdt_sftpToRaw",
    #job_desc: str = "AWS Glue Job with Airflow",
    #script_location: str | None = None,
    #concurrent_run_limit: int | None = None,
    #script_args: dict | None = None,
    #retry_limit: int = 0,
    #num_of_dpus: int | None = None,
    aws_conn_id = "conn_datalabs_aws_prod",
    #region_name: str | None = None,
    #s3_bucket: str | None = None,
    #iam_role_name: str | None = None,
    #create_job_kwargs: dict | None = None,
    #run_job_kwargs: dict | None = None,
    wait_for_completion = True,
    #verbose: bool = False,
    #**kwargs,
)

start_flow >> cbdt_sftpToRaw >> end_flow
