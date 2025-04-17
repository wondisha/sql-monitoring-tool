from db_monitor import SQLMonitor
from config import get_connection_string
import time

def main():
    # Initialize the monitor with your connection string
    monitor = SQLMonitor(get_connection_string())
    
    try:
        while True:
            # Display monitoring results
            monitor.display_monitoring_results()
            
            # Wait for 60 seconds before next check
            print("\nWaiting 60 seconds before next check...")
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")

if __name__ == "__main__":
    main()