
DROP PROCEDURE IF EXISTS dbo.up_taskHub_Create 
GO
DROP PROCEDURE IF EXISTS up_taskHub_Get
GO
DROP TABLE IF EXISTS TaskHub
GO

CREATE TABLE dbo.TaskHub
(
	 TaskHubName	VARCHAR(100) PRIMARY KEY NONCLUSTERED, 
	 CreatedAt		DATETIME2 not null default(getdate()),
	 PartitionCount int default(4)
) WITH (MEMORY_OPTIMIZED=ON)
GO



CREATE PROCEDURE dbo.up_taskHub_Create 
	@taskHubName VARCHAR(100),
	@partitionCount int
WITH NATIVE_COMPILATION, SCHEMABINDING   
AS  	
 
	BEGIN ATOMIC   
	WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  

	declare @exists int
	Select top 1 @exists = 1 from [dbo].[TaskHub] Where TaskHubName = @taskHubName
	if (@exists IS NULL)	
	BEGIN
		INSERT INTO [dbo].[TaskHub] (TaskHubName, PartitionCount) VALUES (@taskHubName, @partitionCount)
	END
END


GO
create PROCEDURE dbo.up_taskHub_Get
	@taskHubName VARCHAR(100)

WITH NATIVE_COMPILATION, SCHEMABINDING   
AS  	
 
	BEGIN ATOMIC   
	WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  

	
	Select 
		TaskHubName		
		, CreatedAt
		, PartitionCount		
	from [dbo].[TaskHub] 
	Where 
		TaskHubName = @taskHubName	
END
