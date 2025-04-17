from db_monitor import SQLMonitor
from interactive_monitor import InteractiveMonitor
from config import get_connection_string

def main():
    # Initialize the monitor
    monitor = SQLMonitor(get_connection_string())
    
    # Create and run the interactive monitor
    interactive = InteractiveMonitor(monitor)
    interactive.run()

if __name__ == "__main__":
    main()