# Use airflow.sdk instead of airflow.decorators
from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="basic_dag",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False
)
def basic_dag():
    
    @task
    def start_task():
        print("Starting the workflow")
        return "started"
    
    @task
    def process_task():
        print("Processing data")
        return "processed"
    
    @task
    def end_task():
        print("Workflow completed")
        return "completed"
    
    # Set dependencies
    start = start_task()
    process = process_task()
    end = end_task()
    
    start >> process >> end

# Create the DAG
dag_instance = basic_dag()