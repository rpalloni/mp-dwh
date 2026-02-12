import subprocess
import typer
import duckdb
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich import print as rprint

console = Console()
project_root = Path("./transformer")

app = typer.Typer()

@app.command()
def ingest():
    """Step 1: Ingest raw data into DuckDB"""
    rprint(Panel("📥 Step 1: Ingesting raw data", style="blue"))
    subprocess.run(["uv", "run", "src/ingest.py"], check=True)
    rprint(Panel("✅ Raw data loaded!", style="green"))

@app.command()
def run():
    """Step 2: Run dbt models"""
    rprint(Panel("🔨 Step 2: Building dbt models", style="blue"))
    subprocess.run(
        ["dbt", "run", "-s", "+dim_mentors", "+dim_users", "+fct_mentoring_sessions", "+fct_booking_sessions"], # MacOs local add "--profiles-dir", ".",
        cwd=project_root, check=True
    )
    rprint(Panel("✅ Mart models built successfully!", style="green"))

@app.command()
def test():
    """Step 3: Run data quality tests"""
    rprint(Panel("🧪 Step 3: Running data quality tests", style="blue"))
    subprocess.run(
        ["dbt", "test"],
        cwd=project_root, check=True
    )
    rprint(Panel("🎉 All tests PASSED!", style="bold green"))

@app.command()
def full():
    """Run complete pipeline"""
    ingest()
    run()
    test()
    rprint(Panel("🚀 Pipeline complete!", style="bold green"))

@app.command()
def report():
    """Run analysis"""
    rprint(Panel("📊 Rebooking Analysis Reports", style="yellow"))
    subprocess.run(["dbt", "run", "-s", "rpt_rebooking_m", "rpt_rebooking_b"], cwd=project_root, check=True) # MacOs local add "--profiles-dir", ".",
    
    con = duckdb.connect("warehouse.duckdb")
    results_mentoring = con.execute("SELECT * FROM dbt_reports.rpt_rebooking_m ORDER BY rebooking_rate_pct DESC").pl()
    results_booking = con.execute("SELECT * FROM dbt_reports.rpt_rebooking_b ORDER BY rebooking_rate_pct DESC").pl()
    
    rprint("Mentoring sessions approach -", results_mentoring)
    rprint("Booking sessions approach -", results_booking)

if __name__ == "__main__":
    app()