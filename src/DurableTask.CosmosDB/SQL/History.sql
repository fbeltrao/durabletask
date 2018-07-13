
DROP PROCEDURE IF EXISTS up_history_Upsert
GO
DROP PROCEDURE IF EXISTS up_history_Get
GO
DROP TABLE IF EXISTS History
GO


CREATE TABLE dbo.History
(	 
	Id BIGINT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
	InstanceId NVARCHAR(400) NOT NULL,  
	Data NVARCHAR(MAX) NOT NULL
) 
GO

CREATE UNIQUE NONCLUSTERED INDEX IX_History_InstanceId ON History (InstanceId)
GO

CREATE PROCEDURE up_history_Get
	@instanceId NVARCHAR(400)
AS
BEGIN
	SET NOCOUNT ON
	SELECT [data] from dbo.History WHERE InstanceId = @instanceId
END
GO

CREATE PROCEDURE up_history_Upsert
	@instanceId NVARCHAR(400),
	@data NVARCHAR(MAX)
AS
BEGIN
	SET NOCOUNT ON	
 	
    MERGE INTO [dbo].[History] AS target
    USING (SELECT @instanceId) AS source (InstanceId)
	ON target.InstanceId = source.InstanceId
    WHEN MATCHED THEN
		UPDATE SET Data = @Data
	WHEN NOT MATCHED THEN
		INSERT (InstanceId, [Data]) VALUES (@instanceId, @Data);
END
