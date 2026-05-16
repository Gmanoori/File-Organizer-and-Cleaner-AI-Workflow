from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'data_processing_pipeline',
    default_args=default_args,
    description='Full Data Cleaning and Ingestion Pipeline',
    schedule_interval=None, # Manual mode
    catchup=False,
    tags=['production', 'pipeline'],
) as dag:

    start = EmptyOperator(task_id='start')

    # --- Step 1.0 ---
    step_1_0 = BashOperator(
        task_id='step_1_0_file_organizer',
        bash_command='bash /opt/airflow/Pipeline/Step_1_0/file_organizer_v3.sh -s /opt/airflow/Data -d /opt/airflow/Organized_Data',
    )

    # --- Step 2.0 ---
    step_2_0 = BashOperator(
        task_id='step_2_0_processor_unstructured',
        bash_command='python /opt/airflow/Pipeline/Step_2_0/processor_unstructured.py',
    )

    # --- Step 3.1 (Parallel) ---
    step_3_1_add_schema = BashOperator(
        task_id='step_3_1_add_schema',
        bash_command='python /opt/airflow/Pipeline/Step_3_1/add_schema_to_csv.py /opt/airflow/Organized_Data/file_inventory.csv',
    )

    step_3_1_call_gemma = BashOperator(
        task_id='step_3_1_call_gemma',
        bash_command='python /opt/airflow/Pipeline/Step_3_1/call_gemma_chat.py',
    )

    step_3_1_hf_spark = BashOperator(
        task_id='step_3_1_hf_spark',
        bash_command='python /opt/airflow/Pipeline/Step_3_1/hf_spark_query.py',
    )

    step_3_1_cleaner_spark = BashOperator(
        task_id='step_3_1_cleaner_spark',
        bash_command='python /opt/airflow/Pipeline/Step_3_1/data_cleaner_spark.py /opt/airflow/Organized_Data/file_inventory.csv',
    )

    step_3_1_truncate_cols = BashOperator(
        task_id='step_3_1_truncate_cols',
        bash_command='python /opt/airflow/Pipeline/Step_3_1/truncate_empty_cols.py',
    )

    # --- Step 4.1 (Parallel) ---
    step_4_1_gather_headers = BashOperator(
        task_id='step_4_1_gather_headers',
        bash_command='python /opt/airflow/Pipeline/Step_4_1/gather_headers.py',
    )

    step_4_1_normalize_clean = BashOperator(
        task_id='step_4_1_normalize_clean',
        bash_command='python /opt/airflow/Pipeline/Step_4_1/normalize_and_clean.py',
    )

    step_4_1_master_joiner = BashOperator(
        task_id='step_4_1_master_joiner',
        bash_command='python /opt/airflow/Pipeline/Step_4_1/master_joiner.py',
    )

    # --- Step 4.2 ---
    step_4_2 = BashOperator(
        task_id='step_4_2_processor_structured',
        bash_command='python /opt/airflow/Pipeline/Step_4_2/processor_structured.py',
    )

    # --- Step 4.3 ---
    step_4_3 = BashOperator(
        task_id='step_4_3_ingest_to_raw',
        bash_command='python /opt/airflow/Pipeline/Step_4_3/ingest_to_raw.py',
    )

    # --- Step 4.4 ---
    step_4_4 = BashOperator(
        task_id='step_4_4_main_geocoder',
        bash_command='python /opt/airflow/Pipeline/Step_4_4/main.py',
    )

    end = EmptyOperator(task_id='end')

    # Define DAG Flow
    start >> step_1_0 >> step_2_0
    
    step_2_0 >> [
        step_3_1_add_schema,
        step_3_1_call_gemma,
        step_3_1_hf_spark,
        step_3_1_cleaner_spark,
        step_3_1_truncate_cols
    ]
    
    [
        step_3_1_add_schema,
        step_3_1_call_gemma,
        step_3_1_hf_spark,
        step_3_1_cleaner_spark,
        step_3_1_truncate_cols
    ] >> step_4_1_gather_headers
    
    [
        step_4_1_gather_headers,
        step_4_1_normalize_clean,
        step_4_1_master_joiner
    ] >> step_4_2 >> step_4_3 >> step_4_4 >> end

    # Note: Step 4.1 normalize and joiner were added to the list. 
    # I'll assume they also depend on Step 3.1 completion.
    [
        step_3_1_add_schema,
        step_3_1_call_gemma,
        step_3_1_hf_spark,
        step_3_1_cleaner_spark,
        step_3_1_truncate_cols
    ] >> step_4_1_normalize_clean
    
    [
        step_3_1_add_schema,
        step_3_1_call_gemma,
        step_3_1_hf_spark,
        step_3_1_cleaner_spark,
        step_3_1_truncate_cols
    ] >> step_4_1_master_joiner
