from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="conditional_simple",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
)
def conditional_simple():
    
    @task
    def task_1():
        print("Task 1 running")
        return "task_1_done"
    
    @task
    def task_2():
        print("Task 2 running")
        return "task_2_done"
    
    # Task 3 will only run if both task_1 and task_2 complete successfully
    @task
    def task_3():
        print("✅ Task 3 running (both Task 1 and Task 2 succeeded)")
        return "task_3_done"
    
    # Task 4 will run if any task fails
    @task(trigger_rule="one_failed")
    def handle_failure():
        print("❌ One of the tasks failed. Task 3 will not run.")
        return "failure_handled"
    
    # Create tasks
    t1 = task_1()
    t2 = task_2()
    t3 = task_3()
    failure_handler = handle_failure()
    
    # Set dependencies - task_3 waits for both
    [t1, t2] >> t3
    
    # Failure handler also waits for both
    [t1, t2] >> failure_handler

conditional_simple = conditional_simple()