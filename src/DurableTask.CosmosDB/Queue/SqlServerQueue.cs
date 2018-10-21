using DurableTask.AzureStorage;
using DurableTask.CosmosDB.Monitoring;
using DurableTask.CosmosDB.Queue;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.CosmosDB
{
    /// <summary>
    /// SQL Server db queue
    /// </summary>
    public class SqlServerQueue : IDisposable, IQueue
    {
        string sqlObjectSuffix = "MemQueue";
        private SqlServerQueueSettings settings;
        bool initialized = false;
        private readonly string queueName;
        private readonly bool isControlQueue;
        private readonly JsonSerializerSettings messageDeserializationSettings;

        //const int MaxTaskMessageLength = 200000;

        /// <summary>
        /// Indicate if completed queue items should be deleted
        /// </summary>
        public bool DeletedCompletedItems { get; set; } = true;

        /// <summary>
        /// Biggest size of message to be handled inline
        /// </summary>
        public int MaxTaskMessageLength { get; set; } = 0;

        /// <summary>
        /// Constructor
        /// </summary>
        public SqlServerQueue(SqlServerQueueSettings settings, string name, bool isControlQueue)
        {
            this.settings = settings;
            this.queueName = name;
            this.isControlQueue = isControlQueue;

            this.messageDeserializationSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Objects
            };

            if (this.isControlQueue)
                this.sqlObjectSuffix = settings.UseMemoryOptimizedTable ? "up_controlmemqueue_" : "up_controlqueue_";
            else
                this.sqlObjectSuffix = settings.UseMemoryOptimizedTable ? "up_workitemmemqueue_" : "up_workitemqueue_";
        }



        internal SqlConnection GetConnection()
        {
            return new SqlConnection(settings.ConnectionString);
        }

        /// <summary>
        /// Enqueues a message
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        internal async Task<SqlServerQueueMessage> Enqueue(SqlServerQueueMessage message)
        {
            if (!this.initialized)
                throw new InvalidOperationException("Must call Initialize() first");

            message.QueueName = this.queueName;
            message.CreatedDate = Utils.ToUnixTime(DateTime.UtcNow);
            if (message.NextVisibleTime <= 0)
                message.NextVisibleTime = Utils.ToUnixTime(DateTime.UtcNow);

            await SaveQueueItem(message);

            return message;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<IQueueMessage>> GetMessagesAsync(int controlQueueBatchSize, TimeSpan controlQueueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            controlQueueBatchSize = Math.Max(controlQueueBatchSize, 20);

            return await DequeueItems(controlQueueBatchSize, controlQueueVisibilityTimeout, cancellationToken);
        }


        async Task<IEnumerable<IQueueMessage>> DequeueItems(int batchSize, TimeSpan visibilityTimeout, CancellationToken cancellationToken)
        {
            try
            {
                var queueVisibilityParameterValue = Utils.ToUnixTime(DateTime.UtcNow.Add(visibilityTimeout));

                List<SqlServerQueueMessage> queuedItems = new List<SqlServerQueueMessage>();
                var sproc = string.Concat(sqlObjectSuffix, "Dequeue");
                using (var recorded = new TelemetryRecorder(sproc, "SQL.Queue"))
                {
                    using (var conn = GetConnection())
                    {
                        var cmd = new SqlCommand(sproc, conn)
                        {
                            CommandType = CommandType.StoredProcedure
                        };

                        cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@batchSize", batchSize) { SqlDbType = SqlDbType.Int });
                        cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@visibilityStarts", queueVisibilityParameterValue) { SqlDbType = SqlDbType.Int });
                        cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@lockDuration", this.QueueItemLockInSeconds) { SqlDbType = SqlDbType.Int });

                        if (isControlQueue)
                            cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@queueName", this.queueName) { SqlDbType = SqlDbType.VarChar, Size = 100 });

                        await conn.OpenAsync();
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var q = new SqlServerQueueMessage()
                                {
                                    QueueItemID = reader.GetInt64(0),
                                    Data = reader.GetString(1),                                    
                                    Version = reader.GetInt32(2),
                                    DequeueCount = reader.GetInt32(3),
                                    CreatedDate = reader.GetInt32(4),
                                    NextVisibleTime = reader.GetInt32(5),
                                    NextAvailableTime = reader.GetInt32(6),
                                    TimeToLive = reader.GetInt32(7)
                                };

                                if (isControlQueue)
                                    q.QueueName = reader.GetString(8);

                                var messageData = JsonConvert.DeserializeObject<MessageData>(q.Data, messageDeserializationSettings);
                                messageData.OriginalQueueMessage = q;
                                messageData.QueueName = q.QueueName ?? this.queueName;
                                messageData.TotalMessageSizeBytes = 0;
                                q.SetObjectData(messageData);

                                queuedItems.Add(q);
                            }
                        }
                    }

                    recorded
                        .AddProperty(nameof(this.queueName), this.queueName)
                        .AsSucceeded();

                    return queuedItems;
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        internal async Task<IQueueMessage> GetMessageAsync(TimeSpan queueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return (await DequeueItems(1, queueVisibilityTimeout, cancellationToken))?.FirstOrDefault();
        }

        IEnumerable<IQueueMessage> BuildQueueItems(IEnumerable<SqlServerQueueMessage> srcQueueItems)
        {
            foreach (var srcQueueItem in srcQueueItems)
            {
                if (!string.IsNullOrEmpty(srcQueueItem.Data))
                {
                    var messageData = JsonConvert.DeserializeObject<MessageData>(srcQueueItem.Data, messageDeserializationSettings);
                    //var dataItem = JsonConvert.DeserializeObject(srcQueueItem.Data, ds);
                    messageData.OriginalQueueMessage = srcQueueItem;
                    messageData.QueueName = srcQueueItem.QueueName;
                    messageData.TotalMessageSizeBytes = 0;

                    //if (!string.IsNullOrEmpty(messageData.CompressedBlobName))
                    //{
                    //    messageData.TaskMessage = await Utils.LoadAttachment<TaskMessage>(this.documentClientWithSerializationSettings, new Uri(messageData.CompressedBlobName, UriKind.Relative));
                    //}

                    srcQueueItem.SetObjectData(messageData);                    
                }
            }


            return srcQueueItems;
        }

        internal async Task CompleteAsync(string id)
        {
            var sproc = $"Delete{sqlObjectSuffix}";

            using (var recorded = new TelemetryRecorder(sproc, "SQL.Queue"))
            {
                using (var conn = GetConnection())
                {
                    var cmd = new SqlCommand(sproc, conn)
                    {
                        CommandType = CommandType.StoredProcedure
                    };

                    cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@QueueItemID", long.Parse(id)) { SqlDbType = SqlDbType.BigInt });

                    await conn.OpenAsync();

                    await cmd.ExecuteNonQueryAsync();
                }

                recorded.AsSucceeded();
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls


        /// <inheritdoc />
        public string Name => this.queueName;

        /// <summary>
        /// Queue item lock in seconds. Default is 60 seconds
        /// </summary>
        public int QueueItemLockInSeconds { get; set; } = 60;

        /// <summary>
        /// Dispose
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                }

                disposedValue = true;
            }
        }


        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <inheritdoc />
        public Task CreateIfNotExistsAsync()
        {
            if (!this.initialized)
            {

                this.initialized = true;
            }

            return Task.FromResult(0);
        }


        /// <inheritdoc />
        public Task<bool> DeleteIfExistsAsync()
        {
            // TODO: delete collection
            return Task.FromResult<bool>(true);
        }


        /// <inheritdoc />
        public async Task DeleteMessageAsync(IQueueMessage queueMessage, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            var sqlQueueMessage = (SqlServerQueueMessage)queueMessage;
            var sproc = string.Concat(sqlObjectSuffix, "Delete");

            using (var recorded = new TelemetryRecorder(sproc, "SQL.Queue"))
            {
                using (var conn = GetConnection())
                {
                    var cmd = new SqlCommand(sproc, conn)
                    {
                        CommandType = CommandType.StoredProcedure
                    };

                    cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@QueueItemID", sqlQueueMessage.QueueItemID) { SqlDbType = SqlDbType.BigInt });
                    cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Version", sqlQueueMessage.Version) { SqlDbType = SqlDbType.Int });

                    await conn.OpenAsync();

                    await cmd.ExecuteNonQueryAsync();
                }

                recorded.AsSucceeded();
            }
        }

        /// <summary>
        /// Helper to save the queue item
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        async Task<SqlServerQueueMessage> SaveQueueItem(SqlServerQueueMessage message)
        {
            if (message.QueueItemID == 0)
            {
                message.CreatedDate = Utils.ToUnixTime(DateTime.UtcNow);
                var sproc = string.Concat(sqlObjectSuffix, "Enqueue");

                using (var recorded = new TelemetryRecorder(sproc, "SQL.Queue"))
                {
                    using (var conn = GetConnection())
                    {
                        var cmd = new SqlCommand(sproc, conn)
                        {
                            CommandType = CommandType.StoredProcedure
                        };

                        cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@NextVisibleTime", message.NextVisibleTime) { SqlDbType = SqlDbType.Int });
                        cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Data", message.Data) { SqlDbType = SqlDbType.NVarChar, Size = -1 });

                        if (isControlQueue)
                            cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@QueueName", message.QueueName) { SqlDbType = SqlDbType.VarChar, Size = 100 });

                        await conn.OpenAsync();
                        var resultObject = await cmd.ExecuteScalarAsync();
                        if (resultObject != null && !Convert.IsDBNull(resultObject))
                        {
                            message.QueueItemID = (long)resultObject;                            
                        }
                    }

                    recorded
                        .AddProperty(nameof(sproc), sproc)
                        .AddProperty(nameof(this.queueName), this.queueName)
                        .AsSucceeded();
                }
            }
            else
            {
                var sproc = string.Concat(sqlObjectSuffix, "Update");

                using (var recorded = new TelemetryRecorder(sproc, "SQL.Queue"))
                {
                    using (var conn = GetConnection())
                    {
                        var cmd = new SqlCommand(sproc, conn)
                        {
                            CommandType = CommandType.StoredProcedure
                        };

                        cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@QueueItemID", message.QueueItemID) { SqlDbType = SqlDbType.BigInt });
                        cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@NextVisibleTime", message.NextVisibleTime) { SqlDbType = SqlDbType.Int });
                        cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@NextAvailableTime", message.NextAvailableTime) { SqlDbType = SqlDbType.Int });
                        cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Version", message.Version) { SqlDbType = SqlDbType.Int });

                        await conn.OpenAsync();

                        var resultObject = await cmd.ExecuteScalarAsync();
                        if (resultObject != null && !Convert.IsDBNull(resultObject))
                        {
                            message.Version = (long)resultObject;
                        }                       
                    }


                    recorded
                        .AddProperty(nameof(sproc), sproc)
                        .AddProperty(nameof(this.queueName), this.queueName)
                        .AsSucceeded();
                }
            }

            return message;
        }


        /// <inheritdoc />
        public async Task UpdateMessageAsync(IQueueMessage originalQueueMessage, TimeSpan controlQueueVisibilityTimeout, MessageUpdateFields updateFields, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            var queueMessage = (SqlServerQueueMessage)originalQueueMessage;
            if (updateFields == MessageUpdateFields.Visibility)
            {
                // We "abandon" the message by settings its visibility timeout to zero.
                // This allows it to be reprocessed on this node or another node.
                queueMessage.NextAvailableTime = queueMessage.NextVisibleTime;

                await SaveQueueItem(queueMessage);
            }
        }

        /// <inheritdoc />
        public Task<IQueueMessage> PeekMessageAsync()
        {
            // TODO: implement it
            var qm = new CosmosDBQueueMessageWrapper();
            return Task.FromResult<IQueueMessage>(qm);
            //throw new NotImplementedException();
        }

        /// <inheritdoc />
        public Task<int> GetQueueLenghtAsync()
        {
            // TODO: Implement it
            return Task.FromResult<int>(0);
        }

        /// <inheritdoc />
        public Task<bool> ExistsAsync()
        {
            // TODO: Implement it
            return Task.FromResult<bool>(true);
        }
        #endregion
    }
}
