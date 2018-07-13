DROP PROCEDURE IF EXISTS dbo.up_workitemqueue_Enqueue
GO
DROP PROCEDURE IF EXISTS dbo.up_workitemqueue_Dequeue
GO
DROP PROCEDURE IF EXISTS dbo.up_workitemqueue_Delete
GO
DROP PROCEDURE IF EXISTS dbo.up_workitemqueue_Update
GO
DROP TABLE IF EXISTS dbo.WorkItemQueue
GO

DROP PROCEDURE IF EXISTS dbo.up_controlqueue_Enqueue
GO
DROP PROCEDURE IF EXISTS dbo.up_controlqueue_Dequeue
GO
DROP PROCEDURE IF EXISTS dbo.up_controlqueue_Delete
GO
DROP PROCEDURE IF EXISTS dbo.up_controlqueue_Update
GO
DROP TABLE IF EXISTS dbo.ControlQueue
GO


CREATE TABLE dbo.ControlQueue
(
	 QueueItemID BIGINT IDENTITY(1,1) PRIMARY KEY CLUSTERED,  
	 CreatedDate INT NOT NULL,
	 NextVisibleTime INT NOT NULL,
	 NextAvailableTime INT NOT NULL,
	 [Data] NVARCHAR(MAX) NOT NULL,
	 QueueName VARCHAR(100) NOT NULL,
	 [Version] INT NOT NULL DEFAULT(0),
	 DequeueCount INT NOT NULL DEFAULT(0),
	 TimeToLive INT NOT NULL DEFAULT(0)
)
GO

CREATE NONCLUSTERED INDEX IX_QueueName_NextAvailableTime ON ControlQueue (QueueName, NextAvailableTime)
GO

CREATE TABLE dbo.WorkItemQueue
(
	 QueueItemID BIGINT IDENTITY(1,1) PRIMARY KEY CLUSTERED,  
	 CreatedDate INT NOT NULL,
	 NextVisibleTime INT NOT NULL,
	 NextAvailableTime INT NOT NULL,
	 [Data] NVARCHAR(MAX) NOT NULL,
	 [Version] INT NOT NULL DEFAULT(0),
	 DequeueCount INT NOT NULL DEFAULT(0),
	 TimeToLive INT NOT NULL DEFAULT(0)
)
GO

CREATE NONCLUSTERED INDEX IX_NextAvailableTime ON WorkItemQueue (NextAvailableTime)
GO


CREATE PROCEDURE dbo.up_controlqueue_Dequeue
	@batchSize int,
	@visibilityStarts int,
	@lockDuration int,
	@queueName VARCHAR(100)
AS  	
	SET NOCOUNT ON
	declare @now int
	set @now = DATEDIFF(s, '1970-01-01 00:00:00', GetDate())

	UPDATE TOP (@batchSize) dbo.ControlQueue
	SET
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
GO  


CREATE PROCEDURE dbo.up_workitemqueue_Dequeue
	@batchSize int,
	@visibilityStarts int,
	@lockDuration int
AS  	
	SET NOCOUNT ON
	declare @now int
	set @now = DATEDIFF(s, '1970-01-01 00:00:00', GetDate())

	UPDATE TOP (@batchSize) dbo.WorkItemQueue
	SET
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
GO  


CREATE PROCEDURE dbo.up_controlqueue_Enqueue
	@Data NVARCHAR(MAX),
	@NextVisibleTime int,
	@QueueName VARCHAR(100)
AS  	
	SET NOCOUNT ON
	declare @now int
	set @now = DATEDIFF(s, '1970-01-01 00:00:00', GetDate())

	INSERT INTO dbo.ControlQueue(CreatedDate, NextVisibleTime, NextAvailableTime, Data, QueueName) 
	OUTPUT Inserted.QueueItemID
	VALUES (@now, @NextVisibleTime, @NextVisibleTime, @Data, @QueueName)
GO

CREATE PROCEDURE dbo.up_workitemqueue_Enqueue 
	@Data NVARCHAR(MAX),
	@NextVisibleTime int
AS  	
	SET NOCOUNT ON
	declare @now int
	set @now = DATEDIFF(s, '1970-01-01 00:00:00', GetDate())

	INSERT INTO dbo.WorkItemQueue (CreatedDate, NextVisibleTime, NextAvailableTime, Data) 
	OUTPUT Inserted.QueueItemID
	VALUES (@now, @NextVisibleTime, @NextVisibleTime, @Data)
GO


CREATE PROCEDURE dbo.up_controlqueue_Update
	@QueueItemID BIGINT,
	@NextVisibleTime int,
	@NextAvailableTime int,	
	@Version INT
AS  	
	SET NOCOUNT ON
	declare @now int
	set @now = DATEDIFF(s, '1970-01-01 00:00:00', GetDate())

	UPDATE dbo.ControlQueue 
	SET 
	    NextVisibleTime=@NextVisibleTime, 
		NextAvailableTime=@NextAvailableTime, 
		Version=Version+1 
	OUTPUT INSERTED.Version
	WHERE 
		QueueItemID=@QueueItemID 
		AND Version=@Version

GO

CREATE PROCEDURE dbo.up_workitemqueue_Update
	@QueueItemID BIGINT,
	@NextVisibleTime int,
	@NextAvailableTime int,
	@Version INT
AS  	
	SET NOCOUNT ON
	declare @now int
	set @now = DATEDIFF(s, '1970-01-01 00:00:00', GetDate())

	UPDATE dbo.WorkItemQueue
	SET 
	    NextVisibleTime=@NextVisibleTime, 
		NextAvailableTime=@NextAvailableTime, 
		Version=Version+1 
	OUTPUT INSERTED.Version
	WHERE 
		QueueItemID=@QueueItemID 
		AND Version=@Version

GO



CREATE PROCEDURE dbo.up_controlqueue_Delete
	@QueueItemID BIGINT,
	@Version INT = null
AS  	
	SET NOCOUNT ON

	DELETE dbo.ControlQueue 	
	WHERE 
		QueueItemID=@QueueItemID 
		AND Version=ISNULL(@Version, Version)


GO


CREATE PROCEDURE dbo.up_workitemqueue_Delete
	@QueueItemID BIGINT,
	@Version INT = null
AS  	
	SET NOCOUNT ON

	DELETE dbo.WorkItemQueue
	WHERE 
		QueueItemID=@QueueItemID 
		AND Version=ISNULL(@Version, Version)
