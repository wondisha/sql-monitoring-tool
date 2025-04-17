import os
from datetime import datetime
from pathlib import Path
from db_monitor import SQLMonitor
from config import get_connection_string
import pandas as pd

def setup_output_directory():
    """Create output directory for monitoring results"""
    base_dir = Path(__file__).parent.parent / 'monitoring_results'
    current_date = datetime.now().strftime('%Y-%m-%d')
    output_dir = base_dir / current_date
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir

def save_monitoring_results(monitor: SQLMonitor, output_dir: Path):
    """Save all monitoring results to CSV files"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Index Analysis
    index_data = monitor.analyze_indexes()
    if not index_data.empty:
        index_data.to_csv(output_dir / f'index_analysis_{timestamp}.csv', index=False)
    
    # Long Running Queries
    long_queries = monitor.get_long_running_queries()
    if not long_queries.empty:
        long_queries.to_csv(output_dir / f'long_running_queries_{timestamp}.csv', index=False)
    
    # Blocking Analysis
    blocking_data = monitor.analyze_blocking()
    if not blocking_data.empty:
        blocking_data.to_csv(output_dir / f'blocking_chains_{timestamp}.csv', index=False)
    
    # Deadlocks
    deadlock_data = monitor.get_deadlocks()
    if not deadlock_data.empty:
        deadlock_data.to_csv(output_dir / f'deadlocks_{timestamp}.csv', index=False)

def main():
    # Initialize the monitor
    monitor = SQLMonitor(get_connection_string())
    
    # Setup output directory
    output_dir = setup_output_directory()
    
    try:
        # Run once and save results
        save_monitoring_results(monitor, output_dir)
        print(f"Monitoring results saved to {output_dir}")
    except Exception as e:
        print(f"Error during monitoring: {e}")
        raise

if __name__ == "__main__":
    main()