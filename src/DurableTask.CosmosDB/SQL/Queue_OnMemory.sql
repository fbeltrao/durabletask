DROP PROCEDURE IF EXISTS dbo.up_workitemmemqueue_Enqueue
GO
DROP PROCEDURE IF EXISTS dbo.up_workitemmemqueue_Dequeue
GO
DROP PROCEDURE IF EXISTS dbo.up_workitemmemqueue_Delete
GO
DROP PROCEDURE IF EXISTS dbo.up_workitemmemqueue_Update
GO
DROP TABLE IF EXISTS dbo.WorkItemMemQueue
GO

DROP PROCEDURE IF EXISTS dbo.up_controlmemqueue_Enqueue
GO
DROP PROCEDURE IF EXISTS dbo.up_controlmemqueue_Dequeue
GO
DROP PROCEDURE IF EXISTS dbo.up_controlmemqueue_Delete
GO
DROP PROCEDURE IF EXISTS dbo.up_controlmemqueue_Update
GO
DROP TABLE IF EXISTS dbo.ControlMemQueue
GO


CREATE TABLE dbo.ControlMemQueue
(
	 QueueItemID BIGINT IDENTITY(1,1) PRIMARY KEY NONCLUSTERED,  
	 CreatedDate INT NOT NULL,
	 NextVisibleTime INT NOT NULL,
	 NextAvailableTime INT NOT NULL,	 
	 [Data] NVARCHAR(MAX) NOT NULL,
	 QueueName VARCHAR(100) NOT NULL,
	 [Version] INT NOT NULL DEFAULT(0),
	 DequeueCount INT NOT NULL DEFAULT(0),
	 TimeToLive INT NOT NULL DEFAULT(0)
) WITH (MEMORY_OPTIMIZED=ON)
GO

ALTER TABLE dbo.ControlMemQueue
	ADD INDEX IX_QueueName_NextAvailableTime (QueueName, NextAvailableTime)
GO

CREATE TABLE dbo.WorkItemMemQueue
(
	 QueueItemID BIGINT IDENTITY(1,1) PRIMARY KEY NONCLUSTERED,  
	 CreatedDate INT NOT NULL,
	 NextVisibleTime INT NOT NULL,
	 NextAvailableTime INT NOT NULL,	
	 [Data] NVARCHAR(MAX) NOT NULL,
	 [Version] INT NOT NULL DEFAULT(0),
	 DequeueCount INT NOT NULL DEFAULT(0),
	 TimeToLive INT NOT NULL DEFAULT(0)
) WITH (MEMORY_OPTIMIZED=ON)
GO

ALTER TABLE dbo.WorkItemMemQueue
	ADD INDEX IX_NextAvailableTime (NextAvailableTime)
GO


CREATE PROCEDURE dbo.up_controlmemqueue_Dequeue
	@batchSize int,
	@visibilityStarts int,
	@lockDuration int,
	@queueName VARCHAR(100)
WITH NATIVE_COMPILATION, SCHEMABINDING   
AS  	
	BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  
		declare @now int
		set @now = DATEDIFF(s, '1970-01-01 00:00:00', GetDate())

		UPDATE TOP (@batchSize) dbo.ControlMemQueue
		SET
			inProgress = 1,
			NextAvailableTime = @now + @lockDuration,
			Version = Version+1,
			DequeueCount = DequeueCount+1
		OUTPUT INSERTED.QueueItemID,
			 INSERTED.[Data],
			 INSERTED.[Version],
			 INSERTED.DequeueCount,
			 INSERTED.CreatedDate,
			 INSERTED.NextVisibleTime,
			 INSERTED.NextAvailableTime,
			 INSERTED.TimeToLive,
			 INSERTED.QueueName

		WHERE		
			QueueName = @queueName
			AND NextAvailableTime <= @now		
	END
GO  


CREATE PROCEDURE dbo.up_workitemmemqueue_Dequeue
	@batchSize int,
	@visibilityStarts int,
	@lockDuration int
WITH NATIVE_COMPILATION, SCHEMABINDING   
AS  	
	BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  
		declare @now int
		set @now = DATEDIFF(s, '1970-01-01 00:00:00', GetDate())

		UPDATE TOP (@batchSize) dbo.WorkItemMemQueue
		SET
			inProgress = 1,
			NextAvailableTime = @now + @lockDuration,
			Version = Version+1,
			DequeueCount = DequeueCount+1
		OUTPUT INSERTED.QueueItemID,
			 INSERTED.[Data],
			 INSERTED.[Version],
			 INSERTED.DequeueCount,
			 INSERTED.CreatedDate,
			 INSERTED.NextVisibleTime,
			 INSERTED.NextAvailableTime,
			 INSERTED.TimeToLive
		WHERE		
			NextAvailableTime <= @now		
	END
GO  



CREATE PROCEDURE dbo.up_controlmemqueue_Enqueue
	@Data NVARCHAR(MAX),
	@NextVisibleTime int,
	@QueueName VARCHAR(100)
WITH NATIVE_COMPILATION, SCHEMABINDING   
AS  	
	BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  
		declare @now int
		set @now = DATEDIFF(s, '1970-01-01 00:00:00', GetDate())

		INSERT INTO dbo.ControlMemQueue(CreatedDate, NextVisibleTime, NextAvailableTime, Data, QueueName) 
		OUTPUT Inserted.QueueItemID
		VALUES (@now, @NextVisibleTime, @NextVisibleTime, @Data, @QueueName)
	END
GO

CREATE PROCEDURE dbo.up_workitemmemqueue_Enqueue 
	@Data NVARCHAR(MAX),
	@NextVisibleTime int
WITH NATIVE_COMPILATION, SCHEMABINDING   
AS  	
	BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  
		declare @now int
		set @now = DATEDIFF(s, '1970-01-01 00:00:00', GetDate())

		INSERT INTO dbo.WorkItemMemQueue (CreatedDate, NextVisibleTime, NextAvailableTime, Data) 
		OUTPUT Inserted.QueueItemID
		VALUES (@now, @NextVisibleTime, @NextVisibleTime, @Data)
	END
GO



CREATE PROCEDURE dbo.up_controlmemqueue_Update
	@QueueItemID BIGINT,
	@NextVisibleTime int,
	@NextAvailableTime int,
	@Version INT
WITH NATIVE_COMPILATION, SCHEMABINDING   
AS  	
	BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  
		declare @now int
		set @now = DATEDIFF(s, '1970-01-01 00:00:00', GetDate())

		UPDATE dbo.ControlMemQueue 
		SET 
			NextVisibleTime=@NextVisibleTime, 
			NextAvailableTime=@NextAvailableTime, 
			Version=Version+1 
		OUTPUT INSERTED.Version
		WHERE 
			QueueItemID=@QueueItemID 
			AND Version=@Version
	END

GO



CREATE PROCEDURE dbo.up_workitemmemqueue_Update
	@QueueItemID BIGINT,
	@NextVisibleTime int,
	@NextAvailableTime int,	
	@Version INT
WITH NATIVE_COMPILATION, SCHEMABINDING   
AS  	
	BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  
		declare @now int
		set @now = DATEDIFF(s, '1970-01-01 00:00:00', GetDate())

		UPDATE dbo.WorkItemMemQueue
		SET 
			NextVisibleTime=@NextVisibleTime, 
			NextAvailableTime=@NextAvailableTime, 			
			Version=Version+1 
		OUTPUT INSERTED.Version
		WHERE 
			QueueItemID=@QueueItemID 
			AND Version=@Version
	END
GO


CREATE PROCEDURE dbo.up_controlmemqueue_Delete
	@QueueItemID BIGINT,
	@Version INT = null
WITH NATIVE_COMPILATION, SCHEMABINDING   
AS  	
	BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  
		DELETE dbo.ControlMemQueue 	
		WHERE 
			QueueItemID=@QueueItemID 
			AND Version=ISNULL(@Version, Version)
	END
GO




CREATE PROCEDURE dbo.up_workitemmemqueue_Delete
	@QueueItemID BIGINT,
	@Version INT = null
WITH NATIVE_COMPILATION, SCHEMABINDING   
AS  	
	BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  

		DELETE dbo.WorkItemMemQueue
		WHERE 
			QueueItemID=@QueueItemID 
			AND Version=ISNULL(@Version, Version)
	END
