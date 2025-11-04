from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

# Configuration
DATA_DIR = "/opt/airflow/data"
RAW_DATA = f"{DATA_DIR}/raw/netflix_titles.csv"
PROCESSED_DIR = f"{DATA_DIR}/processed"

default_args = {
    "owner": "vihaan",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="netflix_spark_pipeline",
    start_date=datetime(2025, 11, 1),
    schedule="@weekly",
    catchup=False,
    default_args=default_args,
    description="ETL pipeline demonstrating PySpark integration with Airflow"
) as dag:

    @task()
    def process_with_pyspark() -> str:
        """Process Netflix data using PySpark"""
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, count, length, trim
        
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        
        print("🚀 Initializing PySpark...")
        
        # Create Spark session
        spark = SparkSession.builder \
            .appName("NetflixPySparkDemo") \
            .master("local[1]") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        print(f"✅ PySpark initialized! Version: {spark.version}")
        
        # Read CSV with PySpark
        df = spark.read.csv(RAW_DATA, header=True, inferSchema=True)
        
        print(f"📊 Loaded {df.count()} rows with PySpark")
        
        # PySpark transformations
        print("\n" + "="*60)
        print("PYSPARK OPERATIONS")
        print("="*60)
        
        # 1. Count by type
        type_counts = df.groupBy("type").count().collect()
        print("\n1. GroupBy aggregation:")
        for row in type_counts:
            print(f"   {row['type']}: {row['count']}")
        
        # 2. Filter operations
        movies = df.filter(col("type") == "Movie").count()
        shows = df.filter(col("type") == "TV Show").count()
        print(f"\n2. Filter operations:")
        print(f"   Movies: {movies}")
        print(f"   TV Shows: {shows}")
        
        # 3. Data cleaning with PySpark
        df_clean = df.select(
            col("show_id"),
            col("type"),
            trim(col("title")).alias("title"),
            col("release_year").cast("string").alias("release_year"),
            col("rating"),
            col("director")
        ).na.fill({"rating": "Unknown", "director": "Unknown"})
        
        print(f"\n3. Cleaned data: {df_clean.count()} rows")
        print("="*60 + "\n")
        
        # Save using PySpark
        output_path = f"{PROCESSED_DIR}/netflix_spark_processed.csv"
        
        # Convert to Pandas for easier saving
        import pandas as pd
        result_df = df_clean.toPandas()
        result_df.to_csv(output_path, index=False)
        
        spark.stop()
        print(f"✅ Processing complete! Saved to: {output_path}")
        
        return output_path

    @task()
    def load_to_postgres(csv_path: str) -> None:
        """Load PySpark-processed data to PostgreSQL"""
        import pandas as pd
        
        df = pd.read_csv(csv_path)
        
        hook = PostgresHook(postgres_conn_id="Postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS netflix_spark;")
            cursor.execute("DROP TABLE IF EXISTS netflix_spark.shows CASCADE;")
            
            # Fixed column sizes
            cursor.execute("""
                CREATE TABLE netflix_spark.shows (
                    show_id VARCHAR(50) PRIMARY KEY,
                    type VARCHAR(50),
                    title TEXT,
                    release_year VARCHAR(50),
                    rating VARCHAR(50),
                    director TEXT
                );
            """)
            
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO netflix_spark.shows 
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (show_id) DO NOTHING;
                """, tuple(row))
            
            conn.commit()
            print(f"✅ Loaded {len(df)} PySpark-processed records to database")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    @task()
    def create_summary() -> dict:
        """Query database and create summary"""
        
        hook = PostgresHook(postgres_conn_id="Postgres")
        
        # Query using PySpark-loaded data
        query = """
            SELECT 
                type,
                COUNT(*) as count
            FROM netflix_spark.shows
            GROUP BY type
        """
        
        df = hook.get_pandas_df(query)
        
        results = {
            "total": int(df['count'].sum()),
            "by_type": df.to_dict('records')
        }
        
        print("\n" + "="*60)
        print("📊 PYSPARK PIPELINE RESULTS")
        print("="*60)
        print(f"Total records processed with PySpark: {results['total']:,}")
        print(f"Breakdown: {results['by_type']}")
        print("✅ Data successfully processed using Apache PySpark")
        print("="*60 + "\n")
        
        return results

    @task()
    def cleanup() -> None:
        """Cleanup"""
        print("✅ PySpark pipeline complete!")

    # Simple pipeline with proper dependencies
    spark_output = process_with_pyspark()
    loaded = load_to_postgres(spark_output)
    summary = create_summary()

    # Make sure summary runs AFTER loaded
    loaded >> summary >> cleanup()