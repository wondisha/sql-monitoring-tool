from datetime import datetime
import sqlalchemy as sa
from sqlalchemy import create_engine, text
import pandas as pd
from rich.console import Console
from rich.table import Table
from typing import List, Dict, Optional, Tuple
import logging
import graphviz
import json
import os
from pathlib import Path

class SQLMonitor:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        self.console = Console()
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        # Create plans directory if it doesn't exist
        self.plans_dir = Path(__file__).parent.parent / 'monitoring_results' / 'execution_plans'
        self.plans_dir.mkdir(parents=True, exist_ok=True)

    def get_user_databases(self) -> List[str]:
        """Get all user databases excluding system databases"""
        query = """
        SELECT name 
        FROM sys.databases 
        WHERE database_id > 4 
        AND state_desc = 'ONLINE'
        AND is_read_only = 0
        """
        try:
            df = pd.read_sql(query, self.engine)
            return df['name'].tolist()
        except Exception as e:
            self.logger.error(f"Error getting user databases: {e}")
            return []

    def get_duplicate_indexes(self) -> pd.DataFrame:
        """Identify duplicate and overlapping indexes"""
        results = []
        with self.engine.connect() as conn:
            for db in self.get_user_databases():
                try:
                    # Switch to user database using proper connection handling
                    conn.execute(sa.text(f"USE [{db}]"))
                    
                    query = """
                    WITH IndexCols AS (
                        SELECT 
                            OBJECT_NAME(i.object_id) as TableName,
                            i.name as IndexName,
                            i.index_id,
                            i.object_id,
                            (SELECT CASE keyno 
                                    WHEN 0 THEN NULL 
                                    ELSE CASE is_included_column 
                                            WHEN 1 THEN NULL 
                                            ELSE COL_NAME(ic.object_id, column_id) 
                                        END 
                                END + ',' 
                            FROM sys.index_columns ic2
                            WHERE ic2.object_id = i.object_id 
                                AND ic2.index_id = i.index_id
                            ORDER BY keyno, is_included_column
                            FOR XML PATH('')) as cols,
                            (SELECT CASE keyno 
                                    WHEN 0 THEN NULL 
                                    ELSE CASE is_included_column 
                                            WHEN 0 THEN NULL 
                                            ELSE COL_NAME(ic.object_id, column_id) 
                                        END 
                                END + ',' 
                            FROM sys.index_columns ic2
                            WHERE ic2.object_id = i.object_id 
                                AND ic2.index_id = i.index_id
                            ORDER BY keyno, is_included_column
                            FOR XML PATH('')) as inc
                        FROM sys.indexes i
                    )
                    SELECT 
                        DB_NAME() as DatabaseName,
                        i1.TableName,
                        i1.IndexName as Index1,
                        i2.IndexName as Index2,
                        i1.cols as Index1Columns,
                        i1.inc as Index1Included,
                        i2.cols as Index2Columns,
                        i2.inc as Index2Included,
                        'Potential duplicate or overlapping indexes' as Suggestion
                    FROM IndexCols i1
                    JOIN IndexCols i2 
                        ON i1.object_id = i2.object_id
                        AND i1.index_id < i2.index_id
                        AND (
                            i1.cols LIKE i2.cols + '%'
                            OR i2.cols LIKE i1.cols + '%'
                        )
                    """
                    
                    df = pd.read_sql(sa.text(query), conn)
                    results.append(df)
                    
                    # Reset back to master database
                    conn.execute(sa.text("USE [master]"))
                    
                except Exception as e:
                    self.logger.error(f"Error getting duplicate indexes for database {db}: {e}")
                    conn.execute(sa.text("USE [master]"))
        
        return pd.concat(results) if results else pd.DataFrame()

    def get_missing_indexes(self) -> pd.DataFrame:
        """Identify missing indexes based on query patterns"""
        query = """
        SELECT
            DB_NAME(d.database_id) as DatabaseName,
            OBJECT_NAME(d.object_id, d.database_id) as TableName,
            d.equality_columns as EqualityColumns,
            d.inequality_columns as InequalityColumns,
            d.included_columns as IncludedColumns,
            s.unique_compiles as NumberOfQueries,
            s.user_seeks + s.user_scans as NumberOfScans,
            CAST(s.avg_total_user_cost as decimal(8,2)) as AvgQueryCostReduction,
            CAST(s.avg_user_impact as decimal(8,2)) as AvgPctBenefit,
            CAST((s.user_seeks + s.user_scans) * s.avg_total_user_cost * (s.avg_user_impact/100.0) as decimal(8,2)) as IndexAdvantage,
            CONCAT('CREATE INDEX [IX_', OBJECT_NAME(d.object_id, d.database_id), '_Missing_',
                    CAST(ROW_NUMBER() OVER(PARTITION BY d.object_id ORDER BY (s.user_seeks + s.user_scans) * s.avg_total_user_cost * (s.avg_user_impact/100.0) DESC) as varchar),
                    '] ON ', OBJECT_NAME(d.object_id, d.database_id), 
                    ' (', ISNULL(d.equality_columns, ''), 
                    CASE WHEN d.equality_columns IS NOT NULL AND d.inequality_columns IS NOT NULL THEN ',' ELSE '' END,
                    ISNULL(d.inequality_columns, ''), ')',
                    CASE WHEN d.included_columns IS NOT NULL THEN ' INCLUDE (' + d.included_columns + ')' ELSE '' END
            ) as CreateIndexStatement
        FROM sys.dm_db_missing_index_details d
        JOIN sys.dm_db_missing_index_groups g ON d.index_handle = g.index_handle
        JOIN sys.dm_db_missing_index_group_stats s ON g.index_group_handle = s.group_handle
        WHERE d.database_id > 4
        ORDER BY IndexAdvantage DESC
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            self.logger.error(f"Error getting missing indexes: {e}")
            return pd.DataFrame()

    def get_unused_indexes(self) -> pd.DataFrame:
        """Identify unused and rarely used indexes"""
        results = []
        with self.engine.connect() as conn:
            for db in self.get_user_databases():
                try:
                    # Switch to user database using proper connection handling
                    conn.execute(sa.text(f"USE [{db}]"))
                    
                    query = """
                    SELECT 
                        DB_NAME() as DatabaseName,
                        OBJECT_SCHEMA_NAME(i.object_id) as SchemaName,
                        OBJECT_NAME(i.object_id) as TableName,
                        i.name as IndexName,
                        i.type_desc as IndexType,
                        ISNULL(us.user_seeks, 0) as UserSeeks,
                        ISNULL(us.user_scans, 0) as UserScans,
                        ISNULL(us.user_lookups, 0) as UserLookups,
                        ISNULL(us.user_updates, 0) as UserUpdates,
                        CASE 
                            WHEN us.user_seeks = 0 AND us.user_scans = 0 AND us.user_lookups = 0 
                            THEN 'Consider dropping this unused index'
                            WHEN us.user_seeks = 0 AND us.user_scans > 0 
                            THEN 'Index is only being scanned, might need optimization'
                            WHEN us.user_updates > (us.user_seeks + us.user_scans + us.user_lookups) * 10
                            THEN 'High update cost relative to reads, consider redesigning'
                            ELSE 'Index is being used effectively'
                        END as Recommendation,
                        CONCAT('DROP INDEX ', i.name, ' ON ', OBJECT_SCHEMA_NAME(i.object_id), '.', OBJECT_NAME(i.object_id)) as DropStatement
                    FROM sys.indexes i
                    LEFT JOIN sys.dm_db_index_usage_stats us
                        ON us.object_id = i.object_id
                        AND us.index_id = i.index_id
                        AND us.database_id = DB_ID()
                    WHERE i.type_desc != 'HEAP'
                        AND i.is_primary_key = 0
                        AND i.is_unique_constraint = 0
                    ORDER BY (ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0))
                    """
                    
                    df = pd.read_sql(sa.text(query), conn)
                    results.append(df)
                    
                    # Reset back to master database
                    conn.execute(sa.text("USE [master]"))
                    
                except Exception as e:
                    self.logger.error(f"Error getting unused indexes for database {db}: {e}")
                    conn.execute(sa.text("USE [master]"))
        
        return pd.concat(results) if results else pd.DataFrame()

    def analyze_indexes(self) -> pd.DataFrame:
        """Analyze index usage and fragmentation across all user databases"""
        results = []
        with self.engine.connect() as conn:
            for db in self.get_user_databases():
                try:
                    # Switch to user database using proper connection handling
                    conn.execute(sa.text(f"USE [{db}]"))
                    
                    query = """
                    SELECT 
                        DB_NAME() as DatabaseName,
                        OBJECT_SCHEMA_NAME(i.object_id) as SchemaName,
                        OBJECT_NAME(i.object_id) as TableName,
                        i.name as IndexName,
                        ips.avg_fragmentation_in_percent as Fragmentation,
                        ISNULL(ius.user_seeks, 0) + ISNULL(ius.user_scans, 0) + ISNULL(ius.user_lookups, 0) as TotalReads,
                        ISNULL(ius.user_updates, 0) as TotalWrites,
                        ius.last_user_seek as LastSeek,
                        CASE 
                            WHEN ips.avg_fragmentation_in_percent >= 30 
                            THEN 'REBUILD INDEX'
                            WHEN ips.avg_fragmentation_in_percent >= 10 
                            THEN 'REORGANIZE INDEX'
                            ELSE 'No action needed'
                        END as MaintenanceAction
                    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
                    JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
                    LEFT JOIN sys.dm_db_index_usage_stats ius 
                        ON ius.database_id = DB_ID()
                        AND ius.object_id = i.object_id 
                        AND ius.index_id = i.index_id
                    WHERE i.type_desc != 'HEAP'
                    """
                    
                    df = pd.read_sql(sa.text(query), conn)
                    results.append(df)
                    
                    # Reset back to master database
                    conn.execute(sa.text("USE [master]"))
                    
                except Exception as e:
                    self.logger.error(f"Error analyzing indexes for database {db}: {e}")
                    # Make sure we return to master even if there's an error
                    conn.execute(sa.text("USE [master]"))
        
        return pd.concat(results) if results else pd.DataFrame()

    def get_long_running_queries(self, threshold_seconds: int = 30) -> pd.DataFrame:
        """Identify long-running queries exceeding the threshold across all databases"""
        query = """
        SELECT 
            s.session_id,
            DB_NAME(r.database_id) as DatabaseName,
            s.login_name as LoginName,
            SUBSTRING(t.text, (r.statement_start_offset/2)+1,
                ((CASE r.statement_end_offset
                    WHEN -1 THEN DATALENGTH(t.text)
                    ELSE r.statement_end_offset
                END - r.statement_start_offset)/2) + 1) as QueryText,
            r.start_time,
            r.total_elapsed_time/1000.0 as DurationSeconds,
            r.cpu_time/1000.0 as CPUTimeSeconds,
            r.logical_reads as LogicalReads,
            r.writes as Writes,
            r.status,
            r.wait_type,
            r.wait_time/1000.0 as WaitTimeSeconds,
            r.last_wait_type
        FROM sys.dm_exec_requests r
        CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
        JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
        WHERE r.session_id != @@SPID
        AND r.database_id > 4  -- Only user databases
        AND r.total_elapsed_time/1000.0 > :threshold
        ORDER BY r.total_elapsed_time DESC
        """
        try:
            return pd.read_sql(text(query), self.engine, params={'threshold': threshold_seconds})
        except Exception as e:
            self.logger.error(f"Error getting long-running queries: {e}")
            return pd.DataFrame()

    def analyze_blocking(self) -> pd.DataFrame:
        """Identify blocking chains and blocked queries across all databases"""
        query = """
        WITH BlockingHierarchy AS (
            SELECT
                w.session_id,
                w.blocking_session_id,
                w.wait_duration_ms/1000.0 as WaitTimeSeconds,
                s.login_name as LoginName,
                DB_NAME(r.database_id) as DatabaseName,
                OBJECT_SCHEMA_NAME(qt.objectid, r.database_id) as SchemaName,
                OBJECT_NAME(qt.objectid, r.database_id) as ObjectName,
                SUBSTRING(t.text, (r.statement_start_offset/2)+1,
                    ((CASE r.statement_end_offset
                        WHEN -1 THEN DATALENGTH(t.text)
                        ELSE r.statement_end_offset
                    END - r.statement_start_offset)/2) + 1) as QueryText,
                r.start_time,
                r.total_elapsed_time/1000.0 as DurationSeconds,
                r.status,
                w.wait_type,
                s.host_name as HostName,
                s.program_name as ProgramName
            FROM sys.dm_os_waiting_tasks w
            JOIN sys.dm_exec_sessions s ON w.session_id = s.session_id
            JOIN sys.dm_exec_requests r ON w.session_id = r.session_id
            CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
            CROSS APPLY (
                SELECT DISTINCT p.objectid 
                FROM sys.dm_exec_query_plan(r.plan_handle) p
            ) as qt
            WHERE w.blocking_session_id IS NOT NULL
            AND r.database_id > 4  -- Only user databases
        )
        SELECT * FROM BlockingHierarchy
        ORDER BY WaitTimeSeconds DESC
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            self.logger.error(f"Error analyzing blocking: {e}")
            return pd.DataFrame()

    def get_deadlocks(self) -> pd.DataFrame:
        """Retrieve recent deadlock information from system health session"""
        query = """
        WITH DeadlockXML AS (
            SELECT CAST(target_data as xml) as DeadlockGraph
            FROM sys.dm_xe_session_targets st
            JOIN sys.dm_xe_sessions s ON s.address = st.event_session_address
            WHERE s.name = 'system_health'
            AND st.target_name = 'ring_buffer'
        )
        SELECT
            deadlock.value('(event/@timestamp)[1]', 'datetime2') as DeadlockTime,
            victim.value('@id', 'varchar(50)') as VictimProcessID,
            CAST(victim.query('.') as nvarchar(max)) as VictimProcess,
            blockingProcess.value('@id', 'varchar(50)') as BlockingProcessID,
            CAST(blockingProcess.query('.') as nvarchar(max)) as BlockingProcess
        FROM DeadlockXML
        CROSS APPLY DeadlockGraph.nodes('//deadlock') AS Deadlocks(deadlock)
        CROSS APPLY deadlock.nodes('//process-list/process') as Victims(victim)
        CROSS APPLY deadlock.nodes('//process-list/process') as BlockingProcesses(blockingProcess)
        WHERE victim.value('@id', 'varchar(50)') != blockingProcess.value('@id', 'varchar(50)')
        ORDER BY DeadlockTime DESC
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            self.logger.error(f"Error getting deadlocks: {e}")
            return pd.DataFrame()

    def analyze_resource_intensive_queries(self) -> pd.DataFrame:
        """Identify queries that are consuming high CPU, memory, or I/O resources"""
        query = """
        SELECT TOP 50
            DB_NAME(qt.dbid) as DatabaseName,
            SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
                ((CASE qs.statement_end_offset
                    WHEN -1 THEN DATALENGTH(qt.text)
                    ELSE qs.statement_end_offset
                END - qs.statement_start_offset)/2) + 1) as QueryText,
            qs.execution_count as ExecutionCount,
            qs.total_worker_time/1000000.0 as TotalCPUSeconds,
            qs.total_worker_time/1000000.0/qs.execution_count as AvgCPUSeconds,
            qs.total_physical_reads as TotalPhysicalReads,
            qs.total_physical_reads/qs.execution_count as AvgPhysicalReads,
            qs.total_logical_reads as TotalLogicalReads,
            qs.total_logical_reads/qs.execution_count as AvgLogicalReads,
            qs.total_logical_writes as TotalLogicalWrites,
            qs.total_logical_writes/qs.execution_count as AvgLogicalWrites,
            qs.total_elapsed_time/1000000.0 as TotalDurationSeconds,
            qs.total_elapsed_time/1000000.0/qs.execution_count as AvgDurationSeconds,
            qs.last_execution_time as LastExecuted,
            CAST(qp.query_plan as nvarchar(max)) as QueryPlan,
            CASE 
                WHEN qs.total_worker_time/qs.execution_count > 1000000 THEN 'High CPU usage'
                WHEN qs.total_physical_reads/qs.execution_count > 1000 THEN 'High disk reads'
                WHEN qs.total_logical_writes/qs.execution_count > 1000 THEN 'High disk writes'
                ELSE 'Normal resource usage'
            END as ResourceAnalysis
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
        CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
        WHERE qt.dbid > 4  -- Exclude system databases
        ORDER BY qs.total_worker_time/qs.execution_count DESC
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            self.logger.error(f"Error analyzing resource intensive queries: {e}")
            return pd.DataFrame()

    def analyze_memory_usage(self) -> pd.DataFrame:
        """Analyze SQL Server memory usage and pressure"""
        query = """
        SELECT 
            CAST(ROUND(physical_memory_kb/1024.0/1024, 2) as decimal(10,2)) as TotalServerMemoryGB,
            CAST(ROUND(virtual_memory_kb/1024.0/1024, 2) as decimal(10,2)) as TotalServerVirtualMemoryGB,
            CAST(ROUND(committed_kb/1024.0/1024, 2) as decimal(10,2)) as SQLServerCommittedGB,
            CAST(ROUND(committed_target_kb/1024.0/1024, 2) as decimal(10,2)) as SQLServerTargetCommittedGB,
            process_physical_memory_low as LowPhysicalMemoryFlag,
            process_virtual_memory_low as LowVirtualMemoryFlag
        FROM sys.dm_os_process_memory;

        SELECT TOP 10
            DB_NAME(database_id) as DatabaseName,
            CAST(ROUND(COUNT(*) * 8/1024.0/1024, 2) as decimal(10,2)) as CacheUsageGB,
            COUNT(*) as BufferPageCount,
            AVG(read_microsec)/1000000.0 as AvgReadTimeSeconds
        FROM sys.dm_os_buffer_descriptors
        WHERE database_id > 4  -- Exclude system databases
        GROUP BY database_id
        ORDER BY BufferPageCount DESC;

        SELECT TOP 10
            OBJECT_NAME(p.object_id) as TableName,
            i.name as IndexName,
            CAST(ROUND(COUNT(*) * 8/1024.0/1024, 2) as decimal(10,2)) as CacheUsageGB,
            COUNT(*) as BufferPageCount
        FROM sys.dm_os_buffer_descriptors b
        INNER JOIN sys.allocation_units a ON a.allocation_unit_id = b.allocation_unit_id
        INNER JOIN sys.partitions p ON a.container_id = p.hobt_id
        INNER JOIN sys.indexes i ON p.index_id = i.index_id AND p.object_id = i.object_id
        WHERE b.database_id = DB_ID()
        AND p.object_id > 100  -- Exclude system objects
        GROUP BY p.object_id, i.name
        ORDER BY BufferPageCount DESC;
        """
        try:
            system_memory = pd.read_sql(query.split(';')[0], self.engine)
            database_memory = pd.read_sql(query.split(';')[1], self.engine)
            table_memory = pd.read_sql(query.split(';')[2], self.engine)
            return system_memory, database_memory, table_memory
        except Exception as e:
            self.logger.error(f"Error analyzing memory usage: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    def analyze_io_performance(self) -> pd.DataFrame:
        """Analyze I/O performance and latency"""
        query = """
        SELECT
            DB_NAME(vfs.database_id) as DatabaseName,
            vfs.file_id,
            mf.name as LogicalFileName,
            mf.physical_name as PhysicalFileName,
            vfs.sample_ms/1000.0 as SampleTimeSeconds,
            vfs.num_of_reads as NumberOfReads,
            vfs.num_of_writes as NumberOfWrites,
            vfs.io_stall_read_ms/1000.0 as ReadStallSeconds,
            vfs.io_stall_write_ms/1000.0 as WriteStallSeconds,
            CAST(ROUND(vfs.size_on_disk_bytes/1024.0/1024/1024, 2) as decimal(10,2)) as SizeGB,
            CASE 
                WHEN vfs.num_of_reads = 0 THEN 0 
                ELSE vfs.io_stall_read_ms/vfs.num_of_reads 
            END as AvgReadLatencyMS,
            CASE 
                WHEN vfs.num_of_writes = 0 THEN 0 
                ELSE vfs.io_stall_write_ms/vfs.num_of_writes 
            END as AvgWriteLatencyMS,
            CASE
                WHEN vfs.io_stall_read_ms/vfs.num_of_reads > 20 
                OR vfs.io_stall_write_ms/vfs.num_of_writes > 20
                THEN 'High latency - Check disk performance'
                ELSE 'Normal latency'
            END as PerformanceAnalysis
        FROM sys.dm_io_virtual_file_stats(NULL, NULL) vfs
        JOIN sys.master_files mf 
            ON vfs.database_id = mf.database_id 
            AND vfs.file_id = mf.file_id
        WHERE vfs.database_id > 4  -- Exclude system databases
        ORDER BY (vfs.io_stall_read_ms + vfs.io_stall_write_ms) DESC
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            self.logger.error(f"Error analyzing I/O performance: {e}")
            return pd.DataFrame()

    def analyze_network_stats(self) -> pd.DataFrame:
        """Analyze network performance and connectivity"""
        query = """
        SELECT 
            ec.client_net_address as ClientAddress,
            ec.local_net_address as ServerAddress,
            ec.local_tcp_port as ServerPort,
            s.login_name as LoginName,
            DB_NAME(s.database_id) as DatabaseName,
            s.program_name as Application,
            ec.connect_time as ConnectionStartTime,
            ec.num_reads as NumberOfReads,
            ec.num_writes as NumberOfWrites,
            ec.last_read as LastReadTime,
            ec.last_write as LastWriteTime,
            s.reads as TotalReads,
            s.writes as TotalWrites,
            s.logical_reads as LogicalReads,
            CAST(ROUND(s.bytes_sent/1024.0/1024, 2) as decimal(10,2)) as MBSent,
            CAST(ROUND(s.bytes_received/1024.0/1024, 2) as decimal(10,2)) as MBReceived,
            CASE 
                WHEN s.bytes_sent + s.bytes_received > 1024*1024*100 THEN 'High network usage'
                ELSE 'Normal network usage'
            END as NetworkAnalysis
        FROM sys.dm_exec_connections ec
        JOIN sys.dm_exec_sessions s ON ec.session_id = s.session_id
        WHERE ec.session_id != @@SPID
        AND s.database_id > 4  -- Exclude system databases
        ORDER BY (s.bytes_sent + s.bytes_received) DESC
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            self.logger.error(f"Error analyzing network stats: {e}")
            return pd.DataFrame()

    def analyze_execution_plan(self, query: str, database: str = None) -> Tuple[pd.DataFrame, str]:
        """Get and analyze the execution plan for a query"""
        try:
            with self.engine.connect() as conn:
                if database:
                    conn.execute(sa.text(f"USE [{database}]"))

                # Get the execution plan
                plan_query = f"""
                SET SHOWPLAN_XML ON;
                {query}
                SET SHOWPLAN_XML OFF;
                """
                result = conn.execute(sa.text(plan_query)).fetchone()
                plan_xml = result[0] if result else None

                if not plan_xml:
                    return pd.DataFrame(), ""

                # Parse important metrics from the plan
                metrics_query = f"""
                WITH XMLNAMESPACES (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
                SELECT 
                    n.value('(@PhysicalOp)', 'varchar(100)') as Operation,
                    n.value('(@EstimatedTotalSubtreeCost)', 'float') as EstimatedCost,
                    n.value('(@EstimateRows)', 'float') as EstimatedRows,
                    n.value('(@EstimateIO)', 'float') as EstimatedIO,
                    n.value('(@EstimateCPU)', 'float') as EstimatedCPU,
                    n.value('(@ParallelSubtreeCost)', 'float') as ParallelCost,
                    COALESCE(n.value('(./Warnings/ColumnsWithNoStatistics/@NoStatistics)[1]', 'varchar(max)'), 'None') as MissingStats,
                    n.value('(@LogicalOp)', 'varchar(100)') as LogicalOperation
                FROM (SELECT CAST('{plan_xml}' AS XML)) as p(plan_xml)
                CROSS APPLY plan_xml.nodes('//RelOp') as q(n)
                """
                
                metrics_df = pd.read_sql(sa.text(metrics_query), conn)

                # Reset back to master database if needed
                if database:
                    conn.execute(sa.text("USE [master]"))

                # Generate visual plan using graphviz
                dot = graphviz.Digraph(comment='Execution Plan', format='svg')
                dot.attr(rankdir='TB')

                def add_nodes_from_xml(xml_str: str, dot: graphviz.Digraph) -> None:
                    """Recursively add nodes to the graph from XML"""
                    from xml.etree import ElementTree as ET
                    root = ET.fromstring(xml_str)

                    def process_node(node, parent_id=None):
                        node_id = str(hash(str(node)))
                        op_type = node.get('PhysicalOp', 'Unknown')
                        est_rows = node.get('EstimateRows', '0')
                        est_cost = node.get('EstimatedTotalSubtreeCost', '0')
                        
                        label = f"{op_type}\\nRows: {est_rows}\\nCost: {est_cost}"
                        
                        # Color code expensive operations
                        if float(est_cost) > 10:
                            dot.node(node_id, label, color='red', style='filled', fillcolor='#ffcccc')
                        elif float(est_cost) > 1:
                            dot.node(node_id, label, color='orange', style='filled', fillcolor='#ffe6cc')
                        else:
                            dot.node(node_id, label)
                        
                        if parent_id:
                            dot.edge(parent_id, node_id)
                        
                        # Process child nodes
                        for child in node.findall('.//RelOp'):
                            process_node(child, node_id)

                    for rel_op in root.findall('.//RelOp'):
                        process_node(rel_op)

                add_nodes_from_xml(plan_xml, dot)
                
                # Save the plan diagram
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                plan_path = self.plans_dir / f'execution_plan_{timestamp}'
                dot.render(plan_path, cleanup=True)

                return metrics_df, str(plan_path) + '.svg'

        except Exception as e:
            self.logger.error(f"Error analyzing execution plan: {e}")
            return pd.DataFrame(), ""

    def analyze_expensive_queries_with_plans(self) -> List[Dict]:
        """Analyze execution plans for expensive queries in the plan cache"""
        query = """
        SELECT TOP 10
            st.text as query_text,
            qs.execution_count,
            qs.total_worker_time/1000000.0 as total_cpu_seconds,
            qs.total_elapsed_time/1000000.0 as total_duration_seconds,
            qs.total_logical_reads,
            qs.total_physical_reads,
            qp.query_plan
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
        CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
        WHERE st.dbid > 4  -- Exclude system databases
        ORDER BY qs.total_worker_time DESC
        """
        
        try:
            results = []
            expensive_queries = pd.read_sql(query, self.engine)
            
            for _, row in expensive_queries.iterrows():
                # Analyze the execution plan
                metrics_df, plan_path = self.analyze_execution_plan(row['query_text'])
                
                results.append({
                    'query_text': row['query_text'],
                    'execution_count': row['execution_count'],
                    'total_cpu_seconds': row['total_cpu_seconds'],
                    'total_duration_seconds': row['total_duration_seconds'],
                    'total_logical_reads': row['total_logical_reads'],
                    'total_physical_reads': row['total_physical_reads'],
                    'metrics': metrics_df.to_dict('records'),
                    'plan_diagram': plan_path,
                    'analysis': self._analyze_plan_metrics(metrics_df)
                })
            
            return results
        except Exception as e:
            self.logger.error(f"Error analyzing expensive queries: {e}")
            return []

    def _analyze_plan_metrics(self, metrics_df: pd.DataFrame) -> List[str]:
        """Analyze plan metrics and provide recommendations"""
        analysis = []
        
        if metrics_df.empty:
            return ["Unable to analyze execution plan metrics"]

        # Check for expensive operations
        expensive_ops = metrics_df[metrics_df['EstimatedCost'] > 1]
        if not expensive_ops.empty:
            for _, op in expensive_ops.iterrows():
                analysis.append(f"Expensive operation found: {op['Operation']} (Cost: {op['EstimatedCost']:.2f})")

        # Check for missing statistics
        missing_stats = metrics_df[metrics_df['MissingStats'] != 'None']
        if not missing_stats.empty:
            analysis.append("Missing statistics detected - consider updating statistics")

        # Check for expensive parallel operations
        parallel_ops = metrics_df[metrics_df['ParallelCost'].notna() & (metrics_df['ParallelCost'] > 0)]
        if not parallel_ops.empty:
            analysis.append("Query uses parallel execution - consider reviewing parallelism threshold")

        # Analyze row estimates
        high_row_ops = metrics_df[metrics_df['EstimatedRows'] > 10000]
        if not high_row_ops.empty:
            analysis.append("Large number of rows being processed - consider adding indexes or optimizing joins")

        return analysis

    def display_monitoring_results(self):
        """Display all monitoring results in a formatted way"""
        self.console.print("\n[bold blue]SQL Database Monitor Report[/bold blue]")
        
        # Resource Intensive Queries
        resource_queries = self.analyze_resource_intensive_queries()
        if not resource_queries.empty:
            self.console.print("\n[bold red]Resource Intensive Queries[/bold red]")
            table = Table(show_header=True, header_style="bold magenta")
            display_cols = ['DatabaseName', 'QueryText', 'ExecutionCount', 'AvgCPUSeconds', 
                          'AvgPhysicalReads', 'AvgLogicalReads', 'AvgDurationSeconds', 'ResourceAnalysis']
            for col in display_cols:
                table.add_column(col)
            for _, row in resource_queries.iterrows():
                table.add_row(*[str(row[col]) for col in display_cols])
            self.console.print(table)

        # Memory Usage
        system_memory, database_memory, table_memory = self.analyze_memory_usage()
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

        # I/O Performance
        io_stats = self.analyze_io_performance()
        if not io_stats.empty:
            self.console.print("\n[bold green]I/O Performance Analysis[/bold green]")
            table = Table(show_header=True, header_style="bold magenta")
            display_cols = ['DatabaseName', 'LogicalFileName', 'SizeGB', 'NumberOfReads', 
                          'NumberOfWrites', 'AvgReadLatencyMS', 'AvgWriteLatencyMS', 'PerformanceAnalysis']
            for col in display_cols:
                table.add_column(col)
            for _, row in io_stats.iterrows():
                table.add_row(*[str(row[col]) for col in display_cols])
            self.console.print(table)

        # Network Statistics
        network_stats = self.analyze_network_stats()
        if not network_stats.empty:
            self.console.print("\n[bold cyan]Network Usage Analysis[/bold cyan]")
            table = Table(show_header=True, header_style="bold magenta")
            display_cols = ['ClientAddress', 'DatabaseName', 'Application', 'MBSent', 
                          'MBReceived', 'NetworkAnalysis']
            for col in display_cols:
                table.add_column(col)
            for _, row in network_stats.iterrows():
                table.add_row(*[str(row[col]) for col in display_cols])
            self.console.print(table)

        # Index Analysis
        index_data = self.analyze_indexes()
        if not index_data.empty:
            self.console.print("\n[bold green]Index Analysis - Current State[/bold green]")
            table = Table(show_header=True, header_style="bold magenta")
            for col in index_data.columns:
                table.add_column(col)
            for _, row in index_data.iterrows():
                table.add_row(*[str(x) for x in row])
            self.console.print(table)

        # Duplicate Indexes
        duplicate_data = self.get_duplicate_indexes()
        if not duplicate_data.empty:
            self.console.print("\n[bold yellow]Duplicate/Overlapping Indexes[/bold yellow]")
            table = Table(show_header=True, header_style="bold magenta")
            for col in duplicate_data.columns:
                table.add_column(col)
            for _, row in duplicate_data.iterrows():
                table.add_row(*[str(x) for x in row])
            self.console.print(table)

        # Missing Indexes
        missing_data = self.get_missing_indexes()
        if not missing_data.empty:
            self.console.print("\n[bold cyan]Missing Index Recommendations[/bold cyan]")
            table = Table(show_header=True, header_style="bold magenta")
            for col in missing_data.columns:
                table.add_column(col)
            for _, row in missing_data.iterrows():
                table.add_row(*[str(x) for x in row])
            self.console.print(table)

        # Unused Indexes
        unused_data = self.get_unused_indexes()
        if not unused_data.empty:
            self.console.print("\n[bold red]Unused and Inefficient Indexes[/bold red]")
            table = Table(show_header=True, header_style="bold magenta")
            for col in unused_data.columns:
                table.add_column(col)
            for _, row in unused_data.iterrows():
                table.add_row(*[str(x) for x in row])
            self.console.print(table)

        # Long Running Queries
        long_queries = self.get_long_running_queries()
        if not long_queries.empty:
            self.console.print("\n[bold red]Long Running Queries[/bold red]")
            table = Table(show_header=True, header_style="bold magenta")
            for col in long_queries.columns:
                table.add_column(col)
            for _, row in long_queries.iterrows():
                table.add_row(*[str(x) for x in row])
            self.console.print(table)

        # Blocking Analysis
        blocking_data = self.analyze_blocking()
        if not blocking_data.empty:
            self.console.print("\n[bold yellow]Current Blocking Chains[/bold yellow]")
            table = Table(show_header=True, header_style="bold magenta")
            for col in blocking_data.columns:
                table.add_column(col)
            for _, row in blocking_data.iterrows():
                table.add_row(*[str(x) for x in row])
            self.console.print(table)

        # Deadlocks
        deadlock_data = self.get_deadlocks()
        if not deadlock_data.empty:
            self.console.print("\n[bold red]Recent Deadlocks[/bold red]")
            table = Table(show_header=True, header_style="bold magenta")
            for col in deadlock_data.columns:
                table.add_column(col)
            for _, row in deadlock_data.iterrows():
                table.add_row(*[str(x) for x in row])
            self.console.print(table)

        # Execution Plan Analysis
        expensive_queries = self.analyze_expensive_queries_with_plans()
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