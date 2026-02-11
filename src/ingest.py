import duckdb
import polars as pl
from pathlib import Path
from rich.console import Console
from rich.progress import Progress

console = Console()
DATA_PATH = Path("data")

def setup_warehouse():
    con = duckdb.connect("warehouse.duckdb")
    return con

def main():
    with Progress() as progress:
        task = progress.add_task("Ingesting raw data to DWH...", total=3)
        
        con = setup_warehouse()
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        
        # Load users
        users_df = pl.read_csv(DATA_PATH / "users_db_export.csv")
        users_df = users_df.unique(subset=["user_id"], keep="last") # row position in originla df
        con.register("users_df", users_df)
        con.execute("CREATE OR REPLACE TABLE raw.users AS SELECT * FROM users_df")
        progress.advance(task)
        
        # Load events
        events_df = pl.read_json(DATA_PATH / "booking_events.json")
        con.register("events_df", events_df)
        con.execute("CREATE OR REPLACE TABLE raw.events AS SELECT * FROM events_df")
        progress.advance(task)
        
        # Load mentors
        mentors_df = pl.read_csv(DATA_PATH / "mentor_tiers.csv")
        con.register("mentors_df", mentors_df)
        con.execute("CREATE OR REPLACE TABLE raw.mentors AS SELECT * FROM mentors_df")
        progress.advance(task)
    
    console.print("✅ Raw data ingested into warehouse.duckdb", style="bold green")

if __name__ == "__main__":
    main()