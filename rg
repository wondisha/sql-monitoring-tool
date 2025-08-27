-- Step 1: Temporarily remove the classifier function to allow for changes.
-- This is necessary because you cannot alter a function that is currently in use by the Resource Governor.
ALTER RESOURCE GOVERNOR WITH (CLASSIFIER_FUNCTION = NULL);
ALTER RESOURCE GOVERNOR RECONFIGURE;
GO

-- Step 2: Create the Resource Pool. This is a logical container for your resources.
-- The parameters set the maximum CPU and memory usage for this pool.
IF NOT EXISTS (SELECT 1 FROM sys.resource_governor_resource_pools WHERE name = 'AdHocPool')
BEGIN
    CREATE RESOURCE POOL AdHocPool
    WITH (
        MIN_CPU_PERCENT = 0,
        MAX_CPU_PERCENT = 20,
        MIN_MEMORY_PERCENT = 0,
        MAX_MEMORY_PERCENT = 10
    );
END
GO

-- Step 3: Create the Workload Group and correctly assign it to the Resource Pool.
-- This is where the syntax in your original query was incorrect.
-- The keyword 'USING' is essential here.
IF NOT EXISTS (SELECT 1 FROM sys.resource_governor_workload_groups WHERE name = 'AdHocGroup')
BEGIN
    CREATE WORKLOAD GROUP AdHocGroup
    USING AdHocPool;
END
GO

-- Step 4: Re-create the classifier function.
-- Your logic for using LIKE 'TECH%' is correct for the login name you showed.
-- The function returns the name of the workload group.
IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'AdHocClassifier' AND type = 'FN')
BEGIN
    DROP FUNCTION dbo.AdHocClassifier;
END
GO

CREATE FUNCTION dbo.AdHocClassifier()
RETURNS sysname WITH SCHEMABINDING
AS
BEGIN
    DECLARE @workload_group sysname;
    DECLARE @user_name sysname = SUSER_SNAME();

    -- Use a case-insensitive match for the login name using LIKE.
    -- The domain prefix 'TECHPC\' is case-sensitive, but the pattern 'TECH%' is not.
    -- Using the 'LOWER' function is a good practice if you are unsure of the case sensitivity of the login.
    IF (LOWER(@user_name) LIKE 'tech%')
        SET @workload_group = 'AdHocGroup';
    ELSE
        SET @workload_group = 'default';

    RETURN @workload_group;
END;
GO

-- Step 5: Assign the classifier function to the Resource Governor and reconfigure to apply the changes.
ALTER RESOURCE GOVERNOR WITH (CLASSIFIER_FUNCTION = dbo.AdHocClassifier);
ALTER RESOURCE GOVERNOR RECONFIGURE;
GO

-- Step 6: Verify the configuration by checking the current session.
-- You should now see 'AdHocGroup' as the workload group for the techpc\wondt user.
SELECT s.session_id, s.login_name, wg.name AS workload_group_name
FROM sys.dm_exec_sessions AS s
JOIN sys.dm_resource_governor_workload_groups AS wg
ON s.group_id = wg.group_id
WHERE s.session_id = @@SPID;
GO
