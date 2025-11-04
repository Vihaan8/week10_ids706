from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
import shutil

# Configuration
DATA_DIR = "/opt/airflow/data"
RAW_DATA = f"{DATA_DIR}/raw/netflix_titles.csv"
PROCESSED_DIR = f"{DATA_DIR}/processed"

default_args = {
    "owner": "vihaan",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="netflix_data_pipeline",
    start_date=datetime(2025, 11, 1),
    schedule="@weekly",
    catchup=False,
    default_args=default_args,
    description="ETL pipeline for Netflix shows analysis"
) as dag:

    @task()
    def extract_shows() -> str:
        """Extract and clean basic show information"""
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        
        # Read raw data
        df = pd.read_csv(RAW_DATA)
        
        # Clean and select columns for shows table
        shows = df[[
            'show_id', 'type', 'title', 'release_year', 
            'rating', 'duration', 'listed_in', 'description'
        ]].copy()
        
        # Data cleaning
        shows = shows.dropna(subset=['show_id', 'title'])
        shows['release_year'] = pd.to_numeric(shows['release_year'], errors='coerce')
        shows = shows[shows['release_year'] > 1900]
        
        # Fill missing values
        shows['rating'] = shows['rating'].fillna('Not Rated')
        shows['listed_in'] = shows['listed_in'].fillna('Unknown')
        
        output_path = f"{PROCESSED_DIR}/shows_clean.csv"
        shows.to_csv(output_path, index=False)
        
        print(f"✅ Extracted {len(shows)} shows")
        print(f"   Movies: {len(shows[shows['type'] == 'Movie'])}")
        print(f"   TV Shows: {len(shows[shows['type'] == 'TV Show'])}")
        
        return output_path

    @task()
    def extract_directors() -> str:
        """Extract and process director information"""
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        
        # Read raw data
        df = pd.read_csv(RAW_DATA)
        
        # Select relevant columns
        directors_df = df[['show_id', 'title', 'director', 'type']].copy()
        
        # Remove rows with no director
        directors_df = directors_df[directors_df['director'].notna()]
        
        # Split multiple directors
        directors_list = []
        for _, row in directors_df.iterrows():
            if pd.notna(row['director']):
                director_names = [d.strip() for d in str(row['director']).split(',')]
                for director in director_names:
                    directors_list.append({
                        'show_id': row['show_id'],
                        'title': row['title'],
                        'director_name': director,
                        'content_type': row['type']
                    })
        
        directors_clean = pd.DataFrame(directors_list)
        directors_clean = directors_clean.drop_duplicates()
        
        output_path = f"{PROCESSED_DIR}/directors_clean.csv"
        directors_clean.to_csv(output_path, index=False)
        
        print(f"✅ Extracted {len(directors_clean)} director-show relationships")
        print(f"   Unique directors: {directors_clean['director_name'].nunique()}")
        
        return output_path

    @task()
    def merge_datasets(shows_path: str, directors_path: str) -> str:
        """Merge shows with director information"""
        
        shows = pd.read_csv(shows_path)
        directors = pd.read_csv(directors_path)
        
        # Merge on show_id
        merged = pd.merge(
            shows,
            directors,
            on='show_id',
            how='left'
        )
        
        # Remove duplicate title column
        if 'title_y' in merged.columns:
            merged = merged.drop('title_y', axis=1)
            merged = merged.rename(columns={'title_x': 'title'})
        
        # Fill missing directors
        merged['director_name'] = merged['director_name'].fillna('Unknown Director')
        
        output_path = f"{PROCESSED_DIR}/netflix_merged.csv"
        merged.to_csv(output_path, index=False)
        
        print(f"✅ Merged dataset: {len(merged)} records")
        return output_path

    @task()
    def load_to_postgres(csv_path: str) -> None:
        """Load merged data into PostgreSQL"""
        
        df = pd.read_csv(csv_path)
        
        hook = PostgresHook(postgres_conn_id="Postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            # Create schema
            cursor.execute("CREATE SCHEMA IF NOT EXISTS netflix_analytics;")
            
            # Drop table if exists
            cursor.execute("DROP TABLE IF EXISTS netflix_analytics.shows CASCADE;")
            
            # Create table
            cursor.execute("""
                CREATE TABLE netflix_analytics.shows (
                    show_id VARCHAR(20) PRIMARY KEY,
                    type VARCHAR(20),
                    title VARCHAR(500),
                    release_year INTEGER,
                    rating VARCHAR(20),
                    duration VARCHAR(20),
                    listed_in TEXT,
                    description TEXT,
                    director_name VARCHAR(200),
                    content_type VARCHAR(20)
                );
            """)
            
            # Insert data
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO netflix_analytics.shows 
                    (show_id, type, title, release_year, rating, duration, 
                     listed_in, description, director_name, content_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (show_id) DO NOTHING;
                """, (
                    row['show_id'], row['type'], row['title'], 
                    int(row['release_year']) if pd.notna(row['release_year']) else None,
                    row['rating'], row['duration'], row['listed_in'], 
                    row['description'], row['director_name'], row['content_type']
                ))
            
            conn.commit()
            print(f"✅ Loaded {len(df)} records to PostgreSQL")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    @task()
    def analyze_data() -> str:
        """Perform analysis and create visualizations"""
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        
        # Read from PostgreSQL
        hook = PostgresHook(postgres_conn_id="Postgres")
        df = hook.get_pandas_df("SELECT * FROM netflix_analytics.shows")
        
        print(f"📊 Analyzing {len(df)} records...")
        
        # Create figure with multiple subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Netflix Content Analysis', fontsize=16, fontweight='bold')
        
        # 1. Content Type Distribution
        content_counts = df['type'].value_counts()
        axes[0, 0].pie(content_counts.values, labels=content_counts.index, 
                       autopct='%1.1f%%', startangle=90)
        axes[0, 0].set_title('Movies vs TV Shows Distribution')
        
        # 2. Top 10 Directors
        top_directors = df[df['director_name'] != 'Unknown Director']['director_name'].value_counts().head(10)
        axes[0, 1].barh(range(len(top_directors)), top_directors.values)
        axes[0, 1].set_yticks(range(len(top_directors)))
        axes[0, 1].set_yticklabels(top_directors.index)
        axes[0, 1].set_xlabel('Number of Shows')
        axes[0, 1].set_title('Top 10 Directors')
        axes[0, 1].invert_yaxis()
        
        # 3. Content by Year
        df['release_year_int'] = pd.to_numeric(df['release_year'], errors='coerce')
        yearly = df.groupby('release_year_int').size()
        recent = yearly[yearly.index >= 2010]
        axes[1, 0].plot(recent.index, recent.values, marker='o', linewidth=2)
        axes[1, 0].set_xlabel('Year')
        axes[1, 0].set_ylabel('Number of Titles')
        axes[1, 0].set_title('Content Release Trend (2010+)')
        axes[1, 0].grid(True, alpha=0.3)
        
        # 4. Top Ratings
        rating_counts = df['rating'].value_counts().head(10)
        axes[1, 1].bar(range(len(rating_counts)), rating_counts.values, color='coral')
        axes[1, 1].set_xticks(range(len(rating_counts)))
        axes[1, 1].set_xticklabels(rating_counts.index, rotation=45, ha='right')
        axes[1, 1].set_ylabel('Count')
        axes[1, 1].set_title('Top 10 Content Ratings')
        
        plt.tight_layout()
        
        viz_path = f"{PROCESSED_DIR}/netflix_analysis.png"
        plt.savefig(viz_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"✅ Analysis saved to {viz_path}")
        return viz_path

    @task()
    def cleanup_intermediate_files() -> None:
        """Clean up intermediate CSV files"""
        
        if os.path.exists(PROCESSED_DIR):
            for filename in os.listdir(PROCESSED_DIR):
                filepath = os.path.join(PROCESSED_DIR, filename)
                if filename.endswith('.csv'):
                    os.remove(filepath)
                    print(f"🗑️  Removed: {filename}")
        
        print("✅ Cleanup completed!")

    # Task dependencies
    shows = extract_shows()
    directors = extract_directors()
    
    merged = merge_datasets(shows, directors)
    loaded = load_to_postgres(merged)
    analysis = analyze_data()
    
    [loaded, analysis] >> cleanup_intermediate_files()