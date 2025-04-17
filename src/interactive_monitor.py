from db_monitor import SQLMonitor
from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.panel import Panel
from rich.table import Table
from rich import print
import time

class InteractiveMonitor:
    def __init__(self, monitor: SQLMonitor):
        self.monitor = monitor
        self.console = Console()
        self.threshold_seconds = 30
        self.refresh_interval = 60

    def show_menu(self):
        print(Panel.fit("[bold blue]SQL Server Interactive Monitor[/bold blue]"))
        print("\n[bold cyan]Available actions:[/bold cyan]")
        print("1. View all metrics")
        print("2. Monitor index health")
        print("3. Track long-running queries")
        print("4. Analyze blocking chains")
        print("5. Check deadlocks")
        print("6. Monitor resource usage")
        print("7. Analyze network performance")
        print("8. Analyze query execution plans")
        print("9. Change settings")
        print("10. Exit")
        
        return Prompt.ask("\nSelect an option", choices=["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"])

    def change_settings(self):
        print("\n[bold cyan]Settings:[/bold cyan]")
        self.threshold_seconds = int(Prompt.ask(
            "Long-running query threshold (seconds)", 
            default=str(self.threshold_seconds)
        ))
        self.refresh_interval = int(Prompt.ask(
            "Refresh interval (seconds)", 
            default=str(self.refresh_interval)
        ))

    def view_all_metrics(self):
        while True:
            self.console.clear()
            self.monitor.display_monitoring_results()
            if not Confirm.ask("\nContinue monitoring?", default=True):
                break
            print(f"\nRefreshing in {self.refresh_interval} seconds...")
            time.sleep(self.refresh_interval)

    def monitor_indexes(self):
        while True:
            self.console.clear()
            print("\n[bold cyan]Index Analysis Options:[/bold cyan]")
            print("1. View all index metrics")
            print("2. Find duplicate/overlapping indexes")
            print("3. Check for missing indexes")
            print("4. Identify unused indexes")
            print("5. Back to main menu")
            
            choice = Prompt.ask("Select an option", choices=["1", "2", "3", "4", "5"])
            
            if choice == "1":
                df = self.monitor.analyze_indexes()
                if not df.empty:
                    print("\n[bold cyan]Sort by:[/bold cyan]")
                    print("1. Fragmentation")
                    print("2. Total Reads")
                    print("3. Total Writes")
                    
                    sort_choice = Prompt.ask("Select sorting option", choices=["1", "2", "3"])
                    
                    if sort_choice == "1":
                        df = df.sort_values("Fragmentation", ascending=False)
                    elif sort_choice == "2":
                        df = df.sort_values("TotalReads", ascending=False)
                    else:
                        df = df.sort_values("TotalWrites", ascending=False)
                    
                    # Display results with maintenance recommendations
                    table = self.console.table(show_header=True, header_style="bold magenta")
                    for col in df.columns:
                        table.add_column(col)
                    for _, row in df.iterrows():
                        table.add_row(*[str(x) for x in row])
                    self.console.print(table)
            
            elif choice == "2":
                df = self.monitor.get_duplicate_indexes()
                if not df.empty:
                    self.console.print("\n[bold yellow]Duplicate/Overlapping Indexes Found:[/bold yellow]")
                    table = self.console.table(show_header=True, header_style="bold magenta")
                    for col in df.columns:
                        table.add_column(col)
                    for _, row in df.iterrows():
                        table.add_row(*[str(x) for x in row])
                    self.console.print(table)
                else:
                    print("\n[green]No duplicate indexes found[/green]")
            
            elif choice == "3":
                df = self.monitor.get_missing_indexes()
                if not df.empty:
                    self.console.print("\n[bold cyan]Missing Index Recommendations:[/bold cyan]")
                    # Sort by IndexAdvantage to show most beneficial indexes first
                    df = df.sort_values("IndexAdvantage", ascending=False)
                    table = self.console.table(show_header=True, header_style="bold magenta")
                    for col in df.columns:
                        table.add_column(col)
                    for _, row in df.iterrows():
                        table.add_row(*[str(x) for x in row])
                    self.console.print(table)
                else:
                    print("\n[green]No missing indexes identified[/green]")
            
            elif choice == "4":
                df = self.monitor.get_unused_indexes()
                if not df.empty:
                    self.console.print("\n[bold red]Unused and Inefficient Indexes:[/bold red]")
                    table = self.console.table(show_header=True, header_style="bold magenta")
                    for col in df.columns:
                        table.add_column(col)
                    for _, row in df.iterrows():
                        table.add_row(*[str(x) for x in row])
                    self.console.print(table)
                else:
                    print("\n[green]No unused indexes found[/green]")
            
            elif choice == "5":
                break
            
            if not Confirm.ask("\nContinue analyzing indexes?", default=True):
                break
            
            if choice != "5":
                print(f"\nRefreshing in {self.refresh_interval} seconds...")
                time.sleep(self.refresh_interval)

    def track_long_queries(self):
        while True:
            self.console.clear()
            df = self.monitor.get_long_running_queries(self.threshold_seconds)
            
            if not df.empty:
                print(f"\n[bold red]Queries running longer than {self.threshold_seconds} seconds:[/bold red]")
                table = self.console.table(show_header=True, header_style="bold magenta")
                for col in df.columns:
                    table.add_column(col)
                for _, row in df.iterrows():
                    table.add_row(*[str(x) for x in row])
                self.console.print(table)
            else:
                print("\n[green]No long-running queries found[/green]")
            
            if not Confirm.ask("\nContinue monitoring long queries?", default=True):
                break
            print(f"\nRefreshing in {self.refresh_interval} seconds...")
            time.sleep(self.refresh_interval)

    def analyze_blocking(self):
        while True:
            self.console.clear()
            df = self.monitor.analyze_blocking()
            
            if not df.empty:
                print("\n[bold yellow]Active Blocking Chains:[/bold yellow]")
                table = self.console.table(show_header=True, header_style="bold magenta")
                for col in df.columns:
                    table.add_column(col)
                for _, row in df.iterrows():
                    table.add_row(*[str(x) for x in row])
                self.console.print(table)
            else:
                print("\n[green]No blocking detected[/green]")
            
            if not Confirm.ask("\nContinue monitoring blocking?", default=True):
                break
            print(f"\nRefreshing in {self.refresh_interval} seconds...")
            time.sleep(self.refresh_interval)

    def check_deadlocks(self):
        while True:
            self.console.clear()
            df = self.monitor.get_deadlocks()
            
            if not df.empty:
                print("\n[bold red]Recent Deadlocks:[/bold red]")
                table = self.console.table(show_header=True, header_style="bold magenta")
                for col in df.columns:
                    table.add_column(col)
                for _, row in df.iterrows():
                    table.add_row(*[str(x) for x in row])
                self.console.print(table)
            else:
                print("\n[green]No deadlocks found[/green]")
            
            if not Confirm.ask("\nContinue checking deadlocks?", default=True):
                break
            print(f"\nRefreshing in {self.refresh_interval} seconds...")
            time.sleep(self.refresh_interval)

    def monitor_resource_usage(self):
        while True:
            self.console.clear()
            print("\n[bold cyan]Resource Monitoring Options:[/bold cyan]")
            print("1. View resource-intensive queries")
            print("2. Check memory usage")
            print("3. Monitor I/O performance")
            print("4. Back to main menu")
            
            choice = Prompt.ask("Select an option", choices=["1", "2", "3", "4"])
            
            if choice == "1":
                df = self.monitor.analyze_resource_intensive_queries()
                if not df.empty:
                    self.console.print("\n[bold red]Resource Intensive Queries[/bold red]")
                    table = Table(show_header=True, header_style="bold magenta")
                    display_cols = ['DatabaseName', 'QueryText', 'ExecutionCount', 'AvgCPUSeconds', 
                                'AvgPhysicalReads', 'AvgLogicalReads', 'AvgDurationSeconds', 'ResourceAnalysis']
                    for col in display_cols:
                        table.add_column(col)
                    for _, row in df.iterrows():
                        table.add_row(*[str(row[col]) for col in display_cols])
                    self.console.print(table)
                else:
                    print("\n[green]No resource-intensive queries found[/green]")
            
            elif choice == "2":
                system_memory, database_memory, table_memory = self.monitor.analyze_memory_usage()
                if not system_memory.empty:
                    self.console.print("\n[bold yellow]System Memory Usage[/bold yellow]")
                    table = Table(show_header=True, header_style="bold magenta")
                    for col in system_memory.columns:
                        table.add_column(col)
                    for _, row in system_memory.iterrows():
                        table.add_row(*[str(x) for x in row])
                    self.console.print(table)

                if not database_memory.empty:
                    self.console.print("\n[bold yellow]Database Memory Usage[/bold yellow]")
                    table = Table(show_header=True, header_style="bold magenta")
                    for col in database_memory.columns:
                        table.add_column(col)
                    for _, row in database_memory.iterrows():
                        table.add_row(*[str(x) for x in row])
                    self.console.print(table)

                if not table_memory.empty:
                    self.console.print("\n[bold yellow]Table Memory Usage[/bold yellow]")
                    table = Table(show_header=True, header_style="bold magenta")
                    for col in table_memory.columns:
                        table.add_column(col)
                    for _, row in table_memory.iterrows():
                        table.add_row(*[str(x) for x in row])
                    self.console.print(table)
            
            elif choice == "3":
                df = self.monitor.analyze_io_performance()
                if not df.empty:
                    self.console.print("\n[bold green]I/O Performance Analysis[/bold green]")
                    table = Table(show_header=True, header_style="bold magenta")
                    display_cols = ['DatabaseName', 'LogicalFileName', 'SizeGB', 'NumberOfReads', 
                                'NumberOfWrites', 'AvgReadLatencyMS', 'AvgWriteLatencyMS', 'PerformanceAnalysis']
                    for col in display_cols:
                        table.add_column(col)
                    for _, row in df.iterrows():
                        table.add_row(*[str(row[col]) for col in display_cols])
                    self.console.print(table)
                else:
                    print("\n[green]No I/O performance issues found[/green]")
            
            elif choice == "4":
                break
            
            if not Confirm.ask("\nContinue monitoring resources?", default=True):
                break
            
            if choice != "4":
                print(f"\nRefreshing in {self.refresh_interval} seconds...")
                time.sleep(self.refresh_interval)

    def analyze_network_performance(self):
        while True:
            self.console.clear()
            df = self.monitor.analyze_network_stats()
            
            if not df.empty:
                self.console.print("\n[bold cyan]Network Usage Analysis[/bold cyan]")
                table = Table(show_header=True, header_style="bold magenta")
                display_cols = ['ClientAddress', 'DatabaseName', 'Application', 'MBSent', 
                            'MBReceived', 'NetworkAnalysis']
                for col in display_cols:
                    table.add_column(col)
                for _, row in df.iterrows():
                    table.add_row(*[str(row[col]) for col in display_cols])
                self.console.print(table)
            else:
                print("\n[green]No network performance issues found[/green]")
            
            if not Confirm.ask("\nContinue monitoring network?", default=True):
                break
            print(f"\nRefreshing in {self.refresh_interval} seconds...")
            time.sleep(self.refresh_interval)

    def analyze_execution_plans(self):
        while True:
            self.console.clear()
            print("\n[bold cyan]Query Execution Plan Analysis:[/bold cyan]")
            print("1. Analyze expensive queries")
            print("2. Analyze specific query")
            print("3. Back to main menu")
            
            choice = Prompt.ask("Select an option", choices=["1", "2", "3"])
            
            if choice == "1":
                expensive_queries = self.monitor.analyze_expensive_queries_with_plans()
                if expensive_queries:
                    self.console.print("\n[bold magenta]Expensive Queries Analysis[/bold magenta]")
                    for idx, query_info in enumerate(expensive_queries, 1):
                        self.console.print(f"\n[cyan]Query #{idx}:[/cyan]")
                        self.console.print("[yellow]Query Text:[/yellow]")
                        self.console.print(query_info['query_text'])
                        self.console.print(f"\n[yellow]Performance Metrics:[/yellow]")
                        self.console.print(f"Execution Count: {query_info['execution_count']}")
                        self.console.print(f"Total CPU Time: {query_info['total_cpu_seconds']:.2f} seconds")
                        self.console.print(f"Total Duration: {query_info['total_duration_seconds']:.2f} seconds")
                        self.console.print(f"Logical Reads: {query_info['total_logical_reads']}")
                        self.console.print(f"Physical Reads: {query_info['total_physical_reads']}")
                        
                        self.console.print("\n[yellow]Plan Analysis:[/yellow]")
                        for insight in query_info['analysis']:
                            self.console.print(f"- {insight}")
                        
                        self.console.print(f"\n[yellow]Plan Diagram:[/yellow] {query_info['plan_diagram']}")
                        self.console.print("\n" + "-"*80)
                else:
                    print("\n[green]No expensive queries found[/green]")
            
            elif choice == "2":
                query = Prompt.ask("\nEnter the SQL query to analyze")
                database = Prompt.ask("Enter database name (optional)", default="")
                
                metrics_df, plan_path = self.monitor.analyze_execution_plan(
                    query, 
                    database if database else None
                )
                
                if not metrics_df.empty:
                    self.console.print("\n[bold cyan]Query Plan Analysis[/bold cyan]")
                    table = Table(show_header=True, header_style="bold magenta")
                    for col in metrics_df.columns:
                        table.add_column(col)
                    for _, row in metrics_df.iterrows():
                        table.add_row(*[str(x) for x in row])
                    self.console.print(table)
                    
                    analysis = self.monitor._analyze_plan_metrics(metrics_df)
                    self.console.print("\n[yellow]Plan Insights:[/yellow]")
                    for insight in analysis:
                        self.console.print(f"- {insight}")
                    
                    if plan_path:
                        self.console.print(f"\n[yellow]Plan Diagram:[/yellow] {plan_path}")
                else:
                    print("\n[red]Unable to generate execution plan[/red]")
            
            elif choice == "3":
                break
            
            if not Confirm.ask("\nContinue analyzing execution plans?", default=True):
                break
            
            if choice != "3":
                print(f"\nRefreshing in {self.refresh_interval} seconds...")
                time.sleep(self.refresh_interval)

    def run(self):
        while True:
            choice = self.show_menu()
            
            if choice == "1":
                self.view_all_metrics()
            elif choice == "2":
                self.monitor_indexes()
            elif choice == "3":
                self.track_long_queries()
            elif choice == "4":
                self.analyze_blocking()
            elif choice == "5":
                self.check_deadlocks()
            elif choice == "6":
                self.monitor_resource_usage()
            elif choice == "7":
                self.analyze_network_performance()
            elif choice == "8":
                self.analyze_execution_plans()
            elif choice == "9":
                self.change_settings()
            else:
                print("\n[bold cyan]Exiting interactive monitor...[/bold cyan]")
                break