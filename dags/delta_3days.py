
from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.interval import DeltaDataIntervalTimetable
import pendulum

@dag(
    dag_id="delta_3days_pipeline",
    start_date=datetime(2024, 2, 1, tz="UTC"),
    schedule=DeltaDataIntervalTimetable(duration(days=3)),  
    catchup=False,
    description="Pipeline runs every 3 days using DeltaDataIntervalTimetable",
    tags=["delta", "3days", "every-3-days"]
)
def delta_3days_pipeline():
    
    @task
    def extract_data():
        """Task 1: Extract data"""
        current_time = pendulum.now("UTC")
        print("=" * 60)
        print(" TASK 1: EXTRACT")
        print("   Schedule: DeltaTriggerTimeTable(duration(days=3))")
        print("   Frequency: Every 3 days")
        print(f"   Current time: {current_time.format('YYYY-MM-DD HH:mm:ss')}")
        print("   Action: Extracting data...")
        print("=" * 60)
        return {"status": "extracted", "records": 1000}
    
    @task
    def transform_data(extracted):
        """Task 2: Transform data"""
        print(" TASK 2: TRANSFORM")
        print(f"   Received {extracted['records']} records")
        print("   Transforming data...")
        return {"status": "transformed", "output": extracted["records"] * 2}
    
    @task
    def load_data(transformed):
        """Task 3: Load data"""
        print(" TASK 3: LOAD")
        print(f"   Loading {transformed['output']} records")
        print("   ✅ Every 3 days pipeline complete!")
        print(f"   Next run: +3 days from now")
        print("=" * 60)
        return "loaded_successfully"
    
    extracted = extract_data()
    transformed = transform_data(extracted)
    loaded = load_data(transformed)

delta_3days_pipeline()

