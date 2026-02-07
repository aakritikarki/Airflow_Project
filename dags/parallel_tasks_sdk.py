from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="parallel_tasks_sdk",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
)
def parallel_tasks_sdk():
    
    @task
    def task_a():
        print("Task A running")
        return "A"
    
    @task
    def task_b():
        print("Task B running")
        return "B"
    
    @task
    def task_c():
        print("Task C running")
        return "C"
    
    @task
    def combine(a, b, c):
        print(f"Combined results: {a}, {b}, {c}")
        return f"{a}+{b}+{c}"
    
    # Parallel execution
    a = task_a()
    b = task_b()
    c = task_c()
    combined = combine(a, b, c)

parallel_tasks_sdk = parallel_tasks_sdk()