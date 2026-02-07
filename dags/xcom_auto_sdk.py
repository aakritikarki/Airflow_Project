from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="xcom_auto_sdk",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
)
def xcom_auto_sdk():
    
    @task
    def extract():
        data = {"numbers": [1, 2, 3, 4, 5]}
        print(f"Extracted: {data}")
        return data
    
    @task
    def transform(data):
        transformed = [x * 2 for x in data["numbers"]]
        result = {"doubled": transformed}
        print(f"Transformed: {result}")
        return result
    
    @task
    def load(transformed_data):
        print(f"Loading: {transformed_data}")
        return f"Loaded {len(transformed_data['doubled'])} items"
    
    # Automatic XCom passing
    raw_data = extract()
    processed_data = transform(raw_data)
    result = load(processed_data)

xcom_auto_sdk = xcom_auto_sdk()