
DROP PROCEDURE IF EXISTS dbo.up_lease_Create 
GO
DROP PROCEDURE IF EXISTS up_lease_List
GO
DROP PROCEDURE IF EXISTS up_lease_Get
GO
DROP PROCEDURE IF EXISTS up_lease_UpdateIf
GO

DROP TABLE IF EXISTS dbo.Lease
GO

CREATE TABLE dbo.Lease
(
	 PartitionID VARCHAR(100) PRIMARY KEY NONCLUSTERED, 
	 Epoch bigint not null default(0),
	 Timeout bigint not null default(0),
	 Owner varchar(100) not null default(''),
	 TaskHubName varchar(100) not null default (''),
	 Token bigint not null default(0)
) WITH (MEMORY_OPTIMIZED=ON)
GO



CREATE PROCEDURE dbo.up_lease_Create 
	@partitionId VARCHAR(100),
	@taskHubName VARCHAR(100),
	@owner VARCHAR(100)  = NULL
WITH NATIVE_COMPILATION, SCHEMABINDING   
AS  	
 
	BEGIN ATOMIC   
	WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  

	declare @exists int
	Select top 1 @exists = 1 from [dbo].[Lease] Where PartitionID = @partitionID AND TaskHubName = @taskHubName
	if (@exists IS NULL)	
	BEGIN
		INSERT INTO [dbo].[Lease] (PartitionID, TaskHubName, Owner) VALUES (@partitionID, @taskHubName, ISNULL(@owner, ''))
	END
END


GO
CREATE PROCEDURE dbo.up_lease_List 
	@taskHubName VARCHAR(100)
WITH NATIVE_COMPILATION, SCHEMABINDING   
AS  	
 
	BEGIN ATOMIC   
	WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  

	
	Select 
		PartitionID
		, Epoch
		, Timeout
		, Owner
		, TaskHubName
		, Token
	from [dbo].[Lease] 
	Where 
		TaskHubName = @taskHubName	
END


GO
CREATE PROCEDURE dbo.up_lease_Get
	@taskHubName VARCHAR(100),
	@partitionID VARCHAR(100)

WITH NATIVE_COMPILATION, SCHEMABINDING   
AS  	
 
	BEGIN ATOMIC   
	WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  

	
	Select 
		PartitionID
		, Epoch
		, Timeout
		, Owner
		, TaskHubName
		, Token
	from [dbo].[Lease] 
	Where 
		PartitionID = @partitionID
		AND TaskHubName = @taskHubName	
END


GO
CREATE PROCEDURE dbo.up_lease_UpdateIf
	@taskHubName VARCHAR(100),
	@partitionID VARCHAR(100),
	@token bigint,
	@leaseTimeout bigint = null,
	@owner VARCHAR(100) = null,
	@epoch bigint = null

WITH NATIVE_COMPILATION, SCHEMABINDING   
AS  	
 
	BEGIN ATOMIC   
	WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  

	
	UPDATE [dbo].[Lease] 
	SET
		Timeout = isnull(@leaseTimeout, Timeout)
		, Owner = isnull(@owner, Owner)
		, Token = Token + 1
		, Epoch = ISNULL(@epoch, Epoch + 1)
	OUTPUT
		INSERTED.Epoch
		, INSERTED.Timeout
		, INSERTED.Owner
		, INSERTED.TaskHubName
		, INSERTED.Token
	WHERE
		PartitionID = @partitionID
		AND TaskHubName = @taskHubName
		AND Token = @token	
END
