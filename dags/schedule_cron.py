
from airflow.sdk import dag, task
from datetime import datetime
import pendulum

@dag(
    dag_id="cron_8am_pipeline",
    start_date=pendulum.datetime(2024, 2, 1, tz="UTC"),
    schedule="0 8 * * *",  # CRON: Minute 0, Hour 8 = 8:00 AM
    catchup=False,
    description="Daily pipeline using cron (runs at 8:00 AM)",
)
def cron_8am_pipeline():
    
    @task
    def extract_data():
        """Task 1: Extract data at 8:00 AM"""
        print("=" * 50)
        print("📥 TASK 1: EXTRACT")
        print("   Schedule: '0 8 * * *' (cron)")
        print("   Time: 8:00 AM")
        print("   Action: Extracting morning data...")
        print("=" * 50)
        return {"status": "extracted", "records": 1000}
    
    @task
    def transform_data(extracted):
        """Task 2: Transform data"""
        print("🔄 TASK 2: TRANSFORM")
        print(f"   Received {extracted['records']} records")
        print("   Transforming morning data...")
        return {"status": "transformed", "output": extracted["records"] * 2}
    
    @task
    def load_data(transformed):
        """Task 3: Load data"""
        print("📤 TASK 3: LOAD")
        print(f"   Loading {transformed['output']} records")
        print("   ✅ Morning 8 AM pipeline complete!")
        print("=" * 50)
        return "loaded_successfully"

    extracted = extract_data()
    transformed = transform_data(extracted)
    loaded = load_data(transformed)
cron_8am_pipeline()