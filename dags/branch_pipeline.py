from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(
    dag_id="branch_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
)
def branch_pipeline():
    
    @task
    def extract_task():
        print("📥 Extracting data...")
        data_source = "s3"  # Change to "db" or "api" to test different paths
        return {"source": data_source, "status": "success"}
    
    def decider_function(**context):
        ti = context.get("ti")
        data = ti.xcom_pull(task_ids="extract_task") if ti else None
        source = data.get("source") if data else None
        
        print(f"🤔 Deciding path for source: {source}")
        
        if source == "s3":
            return "transform_task_s3"
        elif source == "db":
            return "transform_task_db"
        elif source == "api":
            return "transform_task_api"
        else:
            return "transform_task_s3"
    
    decider_task = BranchPythonOperator(
        task_id="decider_task",
        python_callable=decider_function
    )
    
    @task
    def transform_task_s3():
        print("☁️ Transforming S3 data")
        return "s3_transformed"
    
    @task
    def transform_task_db():
        print("🗄️ Transforming Database data")
        return "db_transformed"
    
    @task
    def transform_task_api():
        print("🔌 Transforming API data")
        return "api_transformed"
    
    bash_s3 = BashOperator(
        task_id="bash_s3",
        bash_command='echo "Running bash command for S3"'
    )
    
    @task
    def load_task():
        print("📤 Loading data to destination")
        return "loaded"
    
    @task
    def transform_task_db_final():
        print("⚙️ Final database processing")
        return "db_processed"
    
    @task
    def no_load_task():
        print("⏭️ No load needed for DB")
        return "no_load"
    
    bash_api = BashOperator(
        task_id="bash_api",
        bash_command='echo "Running bash command for API"'
    )
    
    @task
    def skipped_task():
        print("🚫 This path is skipped")
        return "skipped"
    
    extract = extract_task()
    
    transform_s3 = transform_task_s3()
    transform_db = transform_task_db()
    transform_api = transform_task_api()
    
    s3_bash = bash_s3
    s3_load = load_task()
    
    db_final = transform_task_db_final()
    db_no_load = no_load_task()
    
    api_bash = bash_api
    api_skipped = skipped_task()
    
    extract >> decider_task >> [transform_s3, transform_db, transform_api]
    transform_s3 >> s3_bash >> s3_load
    transform_db >> db_final >> db_no_load
    transform_api >> api_bash >> api_skipped
    [s3_load, db_no_load, api_skipped]

branch_pipeline = branch_pipeline()
