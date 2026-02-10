# PRESET: Always runs at midnight
from airflow.sdk import dag, task
from datetime import datetime
import pendulum

@dag(
    dag_id="daily_midnight_pipeline",
    start_date=pendulum.datetime(2024, 2, 1, tz="UTC"),
    schedule="@daily",  
    catchup=False,
    description="Daily pipeline using @daily preset (runs at midnight)",
    tags=["preset", "@daily", "midnight"]
)
def daily_midnight_pipeline():
    
    @task
    def extract_data():
        """Task 1: Extract data at midnight"""
        print("=" * 50)
        print("📥 TASK 1: EXTRACT")
        print("   Schedule: @daily (preset)")
        print("   Time: Midnight (00:00)")
        print("   Action: Extracting daily data...")
        print("=" * 50)
        return {"status": "extracted", "records": 1000}
    
    @task
    def transform_data(extracted):
        """Task 2: Transform data"""
        print("🔄 TASK 2: TRANSFORM")
        print(f"   Received {extracted['records']} records")
        print("   Transforming data...")
        return {"status": "transformed", "output": extracted["records"] * 2}
    
    @task
    def load_data(transformed):
        """Task 3: Load data"""
        print("📤 TASK 3: LOAD")
        print(f"   Loading {transformed['output']} records")
        print("   ✅ Daily midnight pipeline complete!")
        print("=" * 50)
        return "loaded_successfully"
    
    extracted = extract_data()
    transformed = transform_data(extracted)
    loaded = load_data(transformed)

daily_midnight_pipeline()