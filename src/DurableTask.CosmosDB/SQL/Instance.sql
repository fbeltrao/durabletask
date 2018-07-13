
DROP PROCEDURE IF EXISTS up_instance_Upsert
GO
DROP PROCEDURE IF EXISTS up_instance_Get
GO
DROP TABLE IF EXISTS Instance
GO


CREATE TABLE dbo.Instance
(	 
	Id BIGINT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
	InstanceId NVARCHAR(400) NOT NULL,  
	Data NVARCHAR(MAX) NOT NULL
) 
GO

CREATE UNIQUE NONCLUSTERED INDEX IX_Instance_InstanceId ON Instance (InstanceId)
GO

CREATE PROCEDURE up_instance_Get
	@instanceId NVARCHAR(400)
AS
BEGIN
	SET NOCOUNT ON
	SELECT [data] from dbo.Instance WHERE InstanceId = @instanceId
END
GO

CREATE PROCEDURE up_instance_Upsert
	@instanceId NVARCHAR(400),
	@data NVARCHAR(MAX)
AS
BEGIN
	SET NOCOUNT ON	
 	
    MERGE INTO [dbo].[Instance] AS target
    USING (SELECT @instanceId) AS source (InstanceId)
	ON target.InstanceId = source.InstanceId
    WHEN MATCHED THEN
		UPDATE SET Data = @Data
	WHEN NOT MATCHED THEN
		INSERT (InstanceId, [Data]) VALUES (@instanceId, @Data);
END
