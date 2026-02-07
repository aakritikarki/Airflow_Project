from airflow.sdk import dag, task
from airflow.sdk.operators.python import BranchPythonOperator
from datetime import datetime

@dag(
    dag_id="conditional_execution_sdk",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
)
def check_both_tasks():
    
    @task
    def task_1():
        print("Task 1 running")
        # Simulate success/failure
        success = True  # Change to False to test failure
        if not success:
            raise Exception("Task 1 failed!")
        return "task_1_success"
    
    @task
    def task_2():
        print("Task 2 running")
        # Simulate success/failure
        success = True  # Change to False to test failure
        if not success:
            raise Exception("Task 2 failed!")
        return "task_2_success"
    
    def check_if_both_succeeded(**context):
        """Check if both previous tasks succeeded"""
        ti = context["ti"]
        
        try:
            # Try to pull results from both tasks
            result1 = ti.xcom_pull(task_ids="task_1")
            result2 = ti.xcom_pull(task_ids="task_2")
            
            print(f"Task 1 result: {result1}")
            print(f"Task 2 result: {result2}")
            
            # If we get here, both tasks succeeded
            return "run_task_3"
            
        except Exception as e:
            print(f"One of the tasks failed: {e}")
            return "handle_failure"
    
    # Branch operator to decide
    check_both = BranchPythonOperator(
        task_id="check_both_completed",
        python_callable=check_if_both_succeeded
    )
    
    @task
    def task_3():
        print("✅ Task 3 running (both task 1 and 2 succeeded)")
        return "task_3_completed"
    
    @task
    def handle_failure():
        print("❌ Either task 1 or task 2 failed. Skipping task 3.")
        return "failure_handled"
    
    @task
    def final_task():
        print("Workflow complete")
        return "done"
    
    # Create tasks
    t1 = task_1()
    t2 = task_2()
    t3_task = task_3()
    failure_handler = handle_failure()
    final = final_task()
    
    # Set dependencies
    t1 >> check_both
    t2 >> check_both
    check_both >> [t3_task, failure_handler]
    [t3_task, failure_handler] >> final

conditional_execution_sdk = conditional_execution_sdk()