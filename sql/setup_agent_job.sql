USE [msdb]
GO

-- First, check if the job already exists and delete it if it does
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = N'SQL_Monitor_Data_Collection')
BEGIN
    EXEC msdb.dbo.sp_delete_job @job_name = N'SQL_Monitor_Data_Collection'
END
GO

-- Create the job
DECLARE @JobID BINARY(16)
EXEC msdb.dbo.sp_add_job 
    @job_name = N'SQL_Monitor_Data_Collection',
    @description = N'Collects SQL Server monitoring data including index stats, blocking, deadlocks, and long-running queries',
    @category_name = N'Data Collector',
    @owner_login_name = N'sa',
    @enabled = 1,
    @notify_level_eventlog = 2,
    @notify_level_email = 2,
    @job_id = @JobID OUTPUT

-- Create the job step to run the Python script
EXEC msdb.dbo.sp_add_jobstep
    @job_id = @JobID,
    @step_name = N'Run SQL Monitor',
    @subsystem = N'CmdExec',
    @command = N'python "$(ESCAPE_SQUOTE(PROJECTDIR))\src\agent_job.py"',
    @database_name = N'master',
    @flags = 0,
    @retry_attempts = 3,
    @retry_interval = 5

-- Create the schedule (every 15 minutes by default)
EXEC msdb.dbo.sp_add_schedule
    @schedule_name = N'SQL_Monitor_Schedule',
    @freq_type = 4, -- Daily
    @freq_interval = 1,
    @freq_subday_type = 4, -- Minutes
    @freq_subday_interval = 15,
    @active_start_date = 20250416, -- Today's date
    @active_start_time = 000000

-- Attach the schedule to the job
EXEC msdb.dbo.sp_attach_schedule
    @job_id = @JobID,
    @schedule_name = N'SQL_Monitor_Schedule'

-- Add the server
EXEC msdb.dbo.sp_add_jobserver
    @job_id = @JobID,
    @server_name = N'(local)'
GO

-- Optional: Set up email notifications if SQL Server has Database Mail configured
/*
EXEC msdb.dbo.sp_add_notification
    @job_id = @JobID,
    @notification_level = 2,
    @operator_name = N'DBA Team'
GO
*/