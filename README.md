# SQL Server Monitoring Tool

A comprehensive SQL Server monitoring tool that provides real-time analysis of database performance, index health, query execution, and resource utilization.

## Features

- **Index Analysis**
  - Monitor fragmentation levels
  - Identify unused and duplicate indexes
  - Get recommendations for missing indexes
  - Track index usage statistics

- **Query Performance**
  - Track long-running queries
  - Analyze execution plans with visual diagrams
  - Identify resource-intensive queries
  - Monitor blocking chains

- **System Health**
  - Memory usage and pressure monitoring
  - I/O performance analysis
  - Network statistics
  - Deadlock detection and analysis

## Prerequisites

- Python 3.7+
- SQL Server 2016+
- ODBC Driver 17 for SQL Server
- Graphviz (for execution plan visualization)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/sql-monitoring-tool.git
   cd sql-monitoring-tool
   ```

2. Create a virtual environment and activate it:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Linux/Mac
   # or
   venv\Scripts\activate     # On Windows
   ```

3. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

4. Install Graphviz:
   - Windows: Download and install from [Graphviz Downloads](https://graphviz.org/download/)
   - Linux: `sudo apt-get install graphviz`
   - Mac: `brew install graphviz`

5. Copy `.env.template` to `.env` and configure your database connection:
   ```env
   DB_DRIVER=ODBC Driver 17 for SQL Server
   DB_SERVER=your_server_name
   DB_NAME=your_database_name
   DB_TRUSTED_CONNECTION=yes
   # If not using Windows Authentication:
   # DB_USER=your_username
   # DB_PASSWORD=your_password
   ```

## Usage

### Interactive Monitoring

Start the interactive monitoring interface:

```bash
python src/interactive.py
```

This provides a menu-driven interface with options to:
- View all metrics
- Monitor index health
- Track long-running queries
- Analyze blocking chains
- Check deadlocks
- Monitor resource usage
- Analyze network performance
- Analyze query execution plans

### Automated Monitoring

For continuous monitoring with default settings:

```bash
python src/example.py
```

### Scheduled Monitoring

#### Windows Task Scheduler

Use `run_monitor.bat` with Windows Task Scheduler to run monitoring at scheduled intervals.

#### SQL Server Agent

1. Modify the server path in `sql/setup_agent_job.sql`
2. Run the script in SQL Server Management Studio to create a SQL Server Agent job

The job will:
- Run every 15 minutes by default
- Store results in the monitoring_results directory
- Log any errors

## Configuration

All configuration settings are stored in `.env`:

- `DB_DRIVER`: SQL Server ODBC driver name
- `DB_SERVER`: SQL Server instance name
- `DB_NAME`: Default database name
- `DB_TRUSTED_CONNECTION`: Use Windows Authentication
- `DB_USER`: SQL Server username (if not using Windows Authentication)
- `DB_PASSWORD`: SQL Server password (if not using Windows Authentication)

## Project Structure

```
├── monitoring_results/    # Monitoring output and logs
├── sql/                  # SQL Server scripts
├── src/                  # Source code
│   ├── agent_job.py      # Automated monitoring job
│   ├── config.py         # Configuration handling
│   ├── db_monitor.py     # Core monitoring functionality
│   ├── example.py        # Example continuous monitoring
│   ├── interactive.py    # Interactive monitoring entry point
│   └── interactive_monitor.py  # Interactive interface
└── tests/               # Test files
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Thanks to the SQL Server community for DMV insights
- [Rich](https://github.com/Textualize/rich) library for the beautiful CLI interface
- [Graphviz](https://graphviz.org/) for execution plan visualization