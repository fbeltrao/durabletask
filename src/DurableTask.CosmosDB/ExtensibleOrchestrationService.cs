//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

namespace DurableTask.AzureStorage
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Runtime.ExceptionServices;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Partitioning;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.CosmosDB;
    using DurableTask.CosmosDB.Monitoring;
    using DurableTask.CosmosDB.Queue;
    using DurableTask.CosmosDB.Tracking;
    using Microsoft.ApplicationInsights;
    using Microsoft.ApplicationInsights.Extensibility;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;

    /// <summary>
    /// Orchestration service provider for the Durable Task Framework which uses Azure Storage as the durable store.
    /// </summary>
    public class ExtensibleOrchestrationService :
        IOrchestrationService,
        IOrchestrationServiceClient,
        IPartitionObserver,
        IDisposable
    {
        internal static readonly TimeSpan MaxQueuePollingDelay = TimeSpan.FromSeconds(10);

        static readonly HistoryEvent[] EmptyHistoryEventList = new HistoryEvent[0];

        readonly internal IExtensibleOrchestrationServiceSettings settings;
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly internal IQueueManager queueManager;

        readonly LinkedList<PendingMessageBatch> pendingOrchestrationMessageBatches;
        readonly ConcurrentDictionary<string, object> activeOrchestrationInstances;

        readonly ITrackingStore trackingStore;

        readonly TableEntityConverter tableEntityConverter;

        readonly BackoffPollingHelper controlQueueBackoff;
        readonly BackoffPollingHelper workItemQueueBackoff;

        readonly ResettableLazy<Task> taskHubCreator;
        readonly ILeaseManager leaseManager;
        readonly IPartitionManager partitionManager;

        readonly object hubCreationLock;
        bool isStarted;
        Task statsLoop;
        CancellationTokenSource shutdownSource;

        /// <summary>
        /// Initializes a new instance of the <see cref="ExtensibleOrchestrationService"/> class.
        /// </summary>
        /// <param name="settings">The settings used to configure the orchestration service.</param>
        public ExtensibleOrchestrationService(StorageOrchestrationServiceSettings settings) : this(settings, null)
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="ExtensibleOrchestrationService"/> class with a custom instance store.
        /// </summary>
        /// <param name="settings">The settings used to configure the orchestration service.</param>
        /// <param name="customInstanceStore">Custom UserDefined Instance store to be used with the AzureStorageOrchestrationService</param>
        public ExtensibleOrchestrationService(IExtensibleOrchestrationServiceSettings settings, IOrchestrationServiceInstanceStore customInstanceStore)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            ValidateSettings(settings);


            if (!string.IsNullOrEmpty(settings.ApplicationInsightsInstrumentationKey))
            {
                var config = new TelemetryConfiguration(settings.ApplicationInsightsInstrumentationKey)
                {                                    
                };

                var backendType = (!string.IsNullOrEmpty(settings.StorageConnectionString)) ? "storage" : "cosmosdb";

                if (!string.IsNullOrEmpty(settings.SqlConnectionString))
                    backendType = settings.SqlQueueUseMemoryOptimizedTable ? "sql/mem" : "sql/disk";

                config.TelemetryInitializers.Add(new CosmosDB.Monitoring.DurableTaskTelemetryInitializer(backendType));

                TelemetryClientProvider.Set(new TelemetryClient(config));
                
            }

            // TBD - decide if we will use Storage or Cosmos DB. For now initialize Storage specific settings. 
            this.settings = settings;
            this.tableEntityConverter = new TableEntityConverter();
            this.stats = new AzureStorageOrchestrationServiceStats();            

            if (!string.IsNullOrEmpty(settings.StorageConnectionString))
                this.queueManager = new StorageQueueManager(settings, stats);
            else if (!string.IsNullOrEmpty(settings.SqlConnectionString))
                this.queueManager = new SqlServerQueueManager(settings, stats);
            else
                this.queueManager = new CosmosDBQueueManager(settings, stats);

            if (customInstanceStore == null)
            {
                if (!string.IsNullOrEmpty(settings.StorageConnectionString))
                {
                    var storageQueueManager = (StorageQueueManager)this.queueManager;
                    this.trackingStore = new AzureTableTrackingStore(settings.TaskHubName, settings.StorageConnectionString, storageQueueManager.messageManager, ((StorageOrchestrationServiceSettings)this.settings).HistoryTableRequestOptions, this.stats);
                }
                else if (!string.IsNullOrEmpty(settings.SqlConnectionString))
                {
                    this.trackingStore = new SqlTrackingStore(settings.SqlConnectionString);
                }
                else
                {
                    this.trackingStore = new CosmosDbTrackingStore(settings.CosmosDBEndpoint, settings.CosmosDBAuthKey, $"{settings.TaskHubName}instance", $"{settings.TaskHubName}history", "durabletask");
                }
            }
            else
            {
                this.trackingStore = new InstanceStoreBackedTrackingStore(customInstanceStore);
            }

            this.pendingOrchestrationMessageBatches = new LinkedList<PendingMessageBatch>();
            this.activeOrchestrationInstances = new ConcurrentDictionary<string, object>(StringComparer.OrdinalIgnoreCase);

            // Queue polling backoff policies
            var minPollingDelayThreshold = TimeSpan.FromMilliseconds(500);
            this.controlQueueBackoff = new BackoffPollingHelper(MaxQueuePollingDelay, minPollingDelayThreshold);
            this.workItemQueueBackoff = new BackoffPollingHelper(MaxQueuePollingDelay, minPollingDelayThreshold);

            this.hubCreationLock = new object();
            this.taskHubCreator = new ResettableLazy<Task>(
                this.GetTaskHubCreatorTask,
                LazyThreadSafetyMode.ExecutionAndPublication);

            if (!string.IsNullOrEmpty(settings.SqlConnectionString))
            {
                this.leaseManager = new SqlLeaseManager(
                                   settings.TaskHubName,
                                   settings.WorkerId,
                                   settings.SqlConnectionString,
                                   settings.LeaseInterval,
                                   settings.LeaseRenewInterval,
                                   this.stats);

                this.partitionManager = new PartitionManager<SqlLease>(
                    settings.CosmosDBLeaseManagementCollection,
                    this.settings.TaskHubName,
                    settings.WorkerId,
                    this.leaseManager,
                    new PartitionManagerOptions
                    {
                        AcquireInterval = settings.LeaseAcquireInterval,
                        RenewInterval = settings.LeaseRenewInterval,
                        LeaseInterval = settings.LeaseInterval,
                    });
            }
            else if (string.IsNullOrEmpty(settings.CosmosDBLeaseManagementCollection))
            {
                this.leaseManager = BlobLeaseManager.GetBlobLeaseManager(
                    settings.TaskHubName,
                    settings.WorkerId,
                    settings.StorageConnectionString,
                    settings.LeaseInterval,
                    settings.LeaseRenewInterval,
                    this.stats);

                this.partitionManager = new PartitionManager<BlobLease>(
                    this.queueManager.StorageName,
                    this.settings.TaskHubName,
                    settings.WorkerId,
                    this.leaseManager,
                    new PartitionManagerOptions
                    {
                        AcquireInterval = settings.LeaseAcquireInterval,
                        RenewInterval = settings.LeaseRenewInterval,
                        LeaseInterval = settings.LeaseInterval,
                    });
            }
            else
            {
                this.leaseManager = new FakeLeaseManager();
                this.partitionManager = new FakePartitionManager();
/*
                this.leaseManager = new CosmosDBLeaseManager(
                    settings.TaskHubName,
                    settings.WorkerId,
                    settings.CosmosDBEndpoint,
                    settings.CosmosDBAuthKey,
                    settings.CosmosDBName,
                    settings.CosmosDBLeaseManagementCollection,
                    settings.CosmosDBLeaseManagementUsePartition,
                    settings.LeaseInterval,
                    settings.LeaseRenewInterval,
                    this.stats);

                this.partitionManager = new PartitionManager<CosmosDBLease>(
                    settings.CosmosDBLeaseManagementCollection,
                    this.settings.TaskHubName,
                    settings.WorkerId,
                    this.leaseManager,
                    new PartitionManagerOptions
                    {
                        AcquireInterval = settings.LeaseAcquireInterval,
                        RenewInterval = settings.LeaseRenewInterval,
                        LeaseInterval = settings.LeaseInterval,
                    });
                    */
            }
        }

        internal string WorkerId => this.settings.WorkerId;

        internal IEnumerable<IQueue> AllControlQueues => this.queueManager.AllControlQueues.Values;

        internal IEnumerable<IQueue> OwnedControlQueues => this.queueManager.OwnedControlQueues.Values;

        internal IQueue WorkItemQueue => this.queueManager.WorkItemQueue;

        internal ITrackingStore TrackingStore => this.trackingStore;

        




        static void ValidateSettings(IExtensibleOrchestrationServiceSettings settings)
        {
            if (settings.ControlQueueBatchSize > 32)
            {
                throw new ArgumentOutOfRangeException(nameof(settings), "The control queue batch size must not exceed 32.");
            }

            if (settings.PartitionCount < 1 || settings.PartitionCount > 16)
            {
                throw new ArgumentOutOfRangeException(nameof(settings), "The number of partitions must be a positive integer and no greater than 16.");
            }

            // TODO: More validation.
        }

        #region IOrchestrationService
        /// <summary>
        /// Gets or sets the maximum number of orchestrations that can be processed concurrently on a single node.
        /// </summary>
        public int MaxConcurrentTaskOrchestrationWorkItems
        {
            get { return this.settings.MaxConcurrentTaskOrchestrationWorkItems; }
        }

        /// <summary>
        /// Gets or sets the maximum number of work items that can be processed concurrently on a single node.
        /// </summary>
        public int MaxConcurrentTaskActivityWorkItems
        {
            get { return this.settings.MaxConcurrentTaskActivityWorkItems; }
        }

        // We always leave the dispatcher counts at one unless we can find a customer workload that requires more.
        /// <inheritdoc />
        public int TaskActivityDispatcherCount { get; } = 1;

        /// <inheritdoc />
        public int TaskOrchestrationDispatcherCount { get; } = 1;

        #region Management Operations (Create/Delete/Start/Stop)
        /// <summary>
        /// Deletes and creates the neccesary Azure Storage resources for the orchestration service.
        /// </summary>
        public async Task CreateAsync()
        {
            await DeleteAsync();
            await this.taskHubCreator.Value;
        }

        /// <summary>
        /// Creates the necessary Azure Storage resources for the orchestration service if they don't already exist.
        /// </summary>
        public Task CreateIfNotExistsAsync()
        {
            return this.taskHubCreator.Value;
        }

        IDisposable MonitorDependency(string dependencyType) => new CosmosDB.Monitoring.TelemetryRecorder(TelemetryClientProvider.TelemetryClient, dependencyType);

        // Internal logic used by the lazy taskHubCreator
        async Task GetTaskHubCreatorTask()
        {
            using (MonitorDependency(nameof(GetTaskHubCreatorTask)))
            {
                var hubInfo = TaskHubInfo.GetTaskHubInfo(this.settings.TaskHubName, this.settings.PartitionCount);
                await this.leaseManager.CreateLeaseStoreIfNotExistsAsync(hubInfo);
                this.stats.StorageRequests.Increment();

                var tasks = new List<Task>();

                tasks.Add(this.trackingStore.CreateAsync());

                tasks.Add(this.WorkItemQueue.CreateIfNotExistsAsync());

                foreach (var controlQueue in this.AllControlQueues)
                {
                    tasks.Add(controlQueue.CreateIfNotExistsAsync());
                    tasks.Add(this.leaseManager.CreateLeaseIfNotExistAsync(controlQueue.Name));
                }

                await Task.WhenAll(tasks.ToArray());
                this.stats.StorageRequests.Increment(tasks.Count);
            }
        }

        /// <summary>
        /// Deletes the Azure Storage resources used by the orchestration service.
        /// </summary>
        public Task DeleteAsync()
        {
            return DeleteAsync(true);
        }

        Task EnsuredCreatedIfNotExistsAsync()
        {
            return this.CreateIfNotExistsAsync();
        }

        /// <inheritdoc />
        public async Task CreateAsync(bool recreateInstanceStore)
        {
            if (recreateInstanceStore)
            {
                await DeleteTrackingStore();

                this.taskHubCreator.Reset();
            }

            await this.taskHubCreator.Value;
        }

        /// <inheritdoc />
        public async Task DeleteAsync(bool deleteInstanceStore)
        {
            var tasks = new List<Task>();
            tasks.Add(this.queueManager.DeleteAsync());

            if (deleteInstanceStore)
            {
                tasks.Add(DeleteTrackingStore());
            }

            // This code will throw if the container doesn't exist.
            tasks.Add(this.leaseManager.DeleteAllAsync().ContinueWith(t =>
            {
                if (t.Exception?.InnerExceptions?.Count > 0)
                {
                    foreach (Exception e in t.Exception.InnerExceptions)
                    {
                        StorageException storageException = e as StorageException;
                        if (storageException == null || storageException.RequestInformation.HttpStatusCode != 404)
                        {
                            ExceptionDispatchInfo.Capture(e).Throw();
                        }
                    }
                }
            }));

            await Task.WhenAll(tasks.ToArray());
            this.stats.StorageRequests.Increment(tasks.Count);
            this.taskHubCreator.Reset();
        }

        private Task DeleteTrackingStore()
        {
            return this.trackingStore.DeleteAsync();
        }

        /// <inheritdoc />
        public async Task StartAsync()
        {
            if (this.isStarted)
            {
                throw new InvalidOperationException("The orchestration service has already started.");
            }

            // Disable nagling to improve storage access latency:
            // https://blogs.msdn.microsoft.com/windowsazurestorage/2010/06/25/nagles-algorithm-is-not-friendly-towards-small-requests/
            // Ad-hoc testing has shown very nice improvements (20%-50% drop in queue message age for simple scenarios).

            await this.trackingStore.StartAsync();
            await this.queueManager.StartAsync();

            this.shutdownSource = new CancellationTokenSource();
            this.statsLoop = Task.Run(() => this.ReportStatsLoop(this.shutdownSource.Token));

            await this.partitionManager.InitializeAsync();
            await this.partitionManager.SubscribeAsync(this);
            await this.partitionManager.StartAsync();

            this.isStarted = true;
        }

        /// <inheritdoc />
        public Task StopAsync()
        {
            return this.StopAsync(isForced: false);
        }

        /// <inheritdoc />
        public async Task StopAsync(bool isForced)
        {
            this.shutdownSource.Cancel();
            await this.statsLoop;
            await this.partitionManager.StopAsync();
            this.isStarted = false;
        }

        async Task ReportStatsLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken);
                    this.ReportStats();
                }
                catch (TaskCanceledException)
                {
                    // shutting down
                    break;
                }
                catch (Exception e)
                {
                    AnalyticsEventSource.Log.GeneralError(
                        this.queueManager.StorageName,
                        this.settings.TaskHubName,
                        $"Unexpected error in {nameof(ReportStatsLoop)}: {e}");
                }
            }

            // Final reporting of stats
            this.ReportStats();
        }

        void ReportStats()
        {
            // The following stats are reported on a per-interval basis.
            long storageRequests = this.stats.StorageRequests.Reset();
            long messagesSent = this.stats.MessagesSent.Reset();
            long messagesRead = this.stats.MessagesRead.Reset();
            long messagesUpdated = this.stats.MessagesUpdated.Reset();
            long tableEntitiesWritten = this.stats.TableEntitiesWritten.Reset();
            long tableEntitiesRead = this.stats.TableEntitiesRead.Reset();

            // The remaining stats are running numbers
            int pendingOrchestratorInstances;
            long pendingOrchestrationMessages;
            lock (this.pendingOrchestrationMessageBatches)
            {
                pendingOrchestratorInstances = this.pendingOrchestrationMessageBatches.Count;
                pendingOrchestrationMessages = this.stats.PendingOrchestratorMessages.Value;
            }

            AnalyticsEventSource.Log.OrchestrationServiceStats(
                this.queueManager.StorageName,
                this.settings.TaskHubName,
                storageRequests,
                messagesSent,
                messagesRead,
                messagesUpdated,
                tableEntitiesWritten,
                tableEntitiesRead,
                pendingOrchestratorInstances,
                pendingOrchestrationMessages,
                this.activeOrchestrationInstances.Count,
                this.stats.ActiveActivityExecutions.Value);
        }

        async Task IPartitionObserver.OnPartitionAcquiredAsync(Lease lease)
        {          
            if (this.queueManager is IPartitionObserver qmgrPartitionObserver)
            {
                await qmgrPartitionObserver.OnPartitionAcquiredAsync(lease);
            }


            var controlQueue = this.queueManager.GetControlQueue(lease.PartitionId);
            await controlQueue.CreateIfNotExistsAsync();
            this.queueManager.OwnedControlQueues[lease.PartitionId] = controlQueue;
            this.queueManager.AllControlQueues[lease.PartitionId] = controlQueue;

        }

        async Task IPartitionObserver.OnPartitionReleasedAsync(Lease lease, CloseReason reason)
        {

            if (this.queueManager is IPartitionObserver qmgrPartitionObserver)
            {
                await qmgrPartitionObserver.OnPartitionReleasedAsync(lease, reason);
            }

            if (!this.queueManager.OwnedControlQueues.TryRemove(lease.PartitionId, out var controlQueue))
            {
                AnalyticsEventSource.Log.PartitionManagerWarning(
                    this.queueManager.StorageName,
                    this.settings.TaskHubName,
                    this.settings.WorkerId,
                    $"Worker ${this.settings.WorkerId} lost a lease '{lease.PartitionId}' but didn't own the queue.");
            }

            //return Utils.CompletedTask;
        }

        // Used for testing
        internal Task<IEnumerable<Lease>> ListLeasesAsync()
        {
            return this.leaseManager.ListLeasesAsync();
        }

       
        #endregion

        #region Orchestration Work Item Methods
        /// <inheritdoc />
        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            await this.EnsuredCreatedIfNotExistsAsync();

            Stopwatch receiveTimeoutStopwatch = Stopwatch.StartNew();
            PendingMessageBatch nextBatch = null;
            while (true)
            {
                List<MessageData> messages = null;

                // Stop dequeuing messages if the buffer gets too full.
                if (this.stats.PendingOrchestratorMessages.Value < this.settings.ControlQueueBufferThreshold)
                {
                    using (MonitorDependency("queueManager.GetMessagesAsync"))
                    {
                        messages = await this.queueManager.GetMessagesAsync(cancellationToken);
                    }
                    
                    this.stats.MessagesRead.Increment(messages.Count);
                    this.stats.PendingOrchestratorMessages.Increment(messages.Count);
                }

                nextBatch = this.StashMessagesAndGetNextBatch((IEnumerable<MessageData>)messages ?? new MessageData[0]);

                if (nextBatch != null)
                {
                    break;
                }

                if (receiveTimeoutStopwatch.Elapsed > receiveTimeout)
                {
                    return null;
                }

                await this.controlQueueBackoff.WaitAsync(cancellationToken);
            }

            this.controlQueueBackoff.Reset();

            ReceivedMessageContext messageContext =
                ReceivedMessageContext.CreateFromReceivedMessageBatch(
                    this.queueManager.StorageName,
                    this.settings.TaskHubName,
                    nextBatch.Messages);

            OrchestrationInstance instance = messageContext.Instance;
            OrchestrationRuntimeState runtimeState = await this.GetOrchestrationRuntimeStateAsync(
                instance.InstanceId,
                instance.ExecutionId,
                cancellationToken);

            


            var orchestrationWorkItem = new TaskOrchestrationWorkItem
            {
                InstanceId = instance.InstanceId,
                NewMessages = nextBatch.Messages.Select(msg => msg.TaskMessage).ToList(),
                OrchestrationRuntimeState = runtimeState,
                LockedUntilUtc = messageContext.GetNextMessageExpirationTimeUtc()
            };

            // Associate this message context with the work item. We'll restore it back later.
            messageContext.TrySave(orchestrationWorkItem);

            foreach (var message in nextBatch.Messages)
            {
                Trace.WriteLine($"MessageBatch: {message.OriginalQueueMessage.AsString()}");
            }


            if (runtimeState.ExecutionStartedEvent != null &&
                runtimeState.OrchestrationStatus != OrchestrationStatus.Running &&
                runtimeState.OrchestrationStatus != OrchestrationStatus.Pending)
            {
                // The instance has already completed. Delete this message batch.
                var controlQueue = await this.GetControlQueueAsync(instance.InstanceId);
                await this.DeleteMessageBatchAsync(messageContext, controlQueue);
                await this.ReleaseTaskOrchestrationWorkItemAsync(orchestrationWorkItem);
                return null;
            }

            

            return orchestrationWorkItem;
        }

        PendingMessageBatch StashMessagesAndGetNextBatch(IEnumerable<MessageData> queueMessages)
        {
            lock (this.pendingOrchestrationMessageBatches)
            {
                LinkedListNode<PendingMessageBatch> node;

                // If the queue is empty, queueMessages will be an empty enumerable and this foreach will be skipped.
                foreach (MessageData data in queueMessages)
                {
                    PendingMessageBatch targetBatch = null;

                    // Walk backwards through the list of batches until we find one with a matching Instance ID.
                    // This is assumed to be more efficient than walking forward if most messages arrive in the queue in groups.
                    node = this.pendingOrchestrationMessageBatches.Last;
                    while (node != null)
                    {
                        PendingMessageBatch batch = node.Value;
                        if (batch.OrchestrationInstanceId == data.TaskMessage.OrchestrationInstance.InstanceId &&
                            batch.OrchestrationExecutionId == data.TaskMessage.OrchestrationInstance.ExecutionId)
                        {
                            targetBatch = batch;
                            break;
                        }

                        node = node.Previous;
                    }

                    if (targetBatch == null)
                    {
                        targetBatch = new PendingMessageBatch();
                        this.pendingOrchestrationMessageBatches.AddLast(targetBatch);
                    }

                    targetBatch.OrchestrationInstanceId = data.TaskMessage.OrchestrationInstance.InstanceId;
                    targetBatch.OrchestrationExecutionId = data.TaskMessage.OrchestrationInstance.ExecutionId;

                    // If a message has been sitting in the buffer for too long, the invisibility timeout may expire and 
                    // it may get dequeued a second time. In such cases, we should replace the existing copy of the message
                    // with the newer copy to ensure it can be deleted successfully after being processed.
                    int i;
                    for (i = 0; i < targetBatch.Messages.Count; i++)
                    {
                        var existingMessage = targetBatch.Messages[i].OriginalQueueMessage;
                        if (existingMessage.Id == data.OriginalQueueMessage.Id)
                        {
                            AnalyticsEventSource.Log.DuplicateMessageDetected(
                                this.queueManager.StorageName,
                                this.settings.TaskHubName,
                                existingMessage.Id,
                                existingMessage.DequeueCount);
                            targetBatch.Messages[i] = data;
                            break;
                        }
                    }

                    if (i >= targetBatch.Messages.Count)
                    {
                        targetBatch.Messages.Add(data);
                    }
                }

                // Pull batches of messages off the linked-list in FIFO order to ensure fairness.
                // Skip over instances which are currently being processed.
                node = this.pendingOrchestrationMessageBatches.First;
                while (node != null)
                {
                    PendingMessageBatch nextBatch = node.Value;
                    if (!this.activeOrchestrationInstances.ContainsKey(nextBatch.OrchestrationInstanceId))
                    {
                        this.activeOrchestrationInstances.TryAdd(nextBatch.OrchestrationInstanceId, null);
                        this.pendingOrchestrationMessageBatches.Remove(node);
                        this.stats.PendingOrchestratorMessages.Increment(-nextBatch.Messages.Count);
                        return nextBatch;
                    }

                    node = node.Next;
                }

                return null;
            }
        }

        async Task<OrchestrationRuntimeState> GetOrchestrationRuntimeStateAsync(
            string instanceId,
            string expectedExecutionId,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            using (MonitorDependency(nameof(GetOrchestrationRuntimeStateAsync)))
                return new OrchestrationRuntimeState(await this.trackingStore.GetHistoryEventsAsync(instanceId, expectedExecutionId, cancellationToken));
        }

        /// <inheritdoc />
        public async Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState orchestrationState)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem, out context))
            {
                // The context doesn't exist - possibly because this is a duplicate message.
                AnalyticsEventSource.Log.AssertFailure(
                    this.queueManager.StorageName,
                    this.settings.TaskHubName,
                    $"Could not find context for orchestration work item with InstanceId = {workItem.InstanceId}.");
                return;
            }

            OrchestrationRuntimeState runtimeState = workItem.OrchestrationRuntimeState;

            string instanceId = workItem.InstanceId;
            string executionId = runtimeState.OrchestrationInstance.ExecutionId;

            using (MonitorDependency("trackingStore.UpdateStateAsync"))
                await this.trackingStore.UpdateStateAsync(runtimeState, instanceId, executionId);

            bool addedControlMessages = false;
            bool addedWorkItemMessages = false;

            var currentControlQueue = await this.GetControlQueueAsync(instanceId);
            int totalMessageCount = 0;

            // Second persistence step is to commit outgoing messages to their respective queues. If there is
            // any failures here, then the messages may get written again later.
            var enqueueTasks = new List<Task>(runtimeState.NewEvents.Count);
            if (orchestratorMessages?.Count > 0)
            {
                totalMessageCount += orchestratorMessages.Count;
                addedControlMessages = true;

                foreach (TaskMessage taskMessage in orchestratorMessages)
                {
                    string targetInstanceId = taskMessage.OrchestrationInstance.InstanceId;
                    var targetControlQueue = await this.GetControlQueueAsync(targetInstanceId);

                    enqueueTasks.Add(this.queueManager.EnqueueMessageAsync(targetControlQueue, context, taskMessage, null, ((StorageOrchestrationServiceSettings)this.settings).ControlQueueRequestOptions));
                }
            }

            if (timerMessages?.Count > 0)
            {
                totalMessageCount += timerMessages.Count;
                addedControlMessages = true;

                foreach (TaskMessage taskMessage in timerMessages)
                {
                    DateTime messageFireTime = ((TimerFiredEvent)taskMessage.Event).FireAt;
                    TimeSpan initialVisibilityDelay = messageFireTime.Subtract(DateTime.UtcNow);
                    Debug.Assert(initialVisibilityDelay <= TimeSpan.FromDays(7));
                    if (initialVisibilityDelay < TimeSpan.Zero)
                    {
                        initialVisibilityDelay = TimeSpan.Zero;
                    }

                    enqueueTasks.Add(this.queueManager.EnqueueMessageAsync(currentControlQueue, context, taskMessage, initialVisibilityDelay, ((StorageOrchestrationServiceSettings)this.settings).ControlQueueRequestOptions));                    
                }
            }

            if (outboundMessages?.Count > 0)
            {
                totalMessageCount += outboundMessages.Count;
                addedWorkItemMessages = true;
                foreach (TaskMessage taskMessage in outboundMessages)
                {
                    enqueueTasks.Add(this.queueManager.EnqueueMessageAsync(this.queueManager.WorkItemQueue, context, taskMessage, null, ((StorageOrchestrationServiceSettings)this.settings).WorkItemQueueRequestOptions));
                }
            }

            if (continuedAsNewMessage != null)
            {
                totalMessageCount++;
                addedControlMessages = true;

                enqueueTasks.Add(this.queueManager.EnqueueMessageAsync(currentControlQueue, context, continuedAsNewMessage, null, ((StorageOrchestrationServiceSettings)this.settings).ControlQueueRequestOptions));
            }

            await Task.WhenAll(enqueueTasks);
            this.stats.StorageRequests.Increment(totalMessageCount);
            this.stats.MessagesSent.Increment(totalMessageCount);

            // Signal queue listeners to start polling immediately to reduce
            // unnecessary wait time between sending and receiving.
            if (addedControlMessages)
            {
                this.controlQueueBackoff.Reset();
            }

            if (addedWorkItemMessages)
            {
                this.workItemQueueBackoff.Reset();
            }

            await this.DeleteMessageBatchAsync(context, currentControlQueue);
        }

       
        async Task DeleteMessageBatchAsync(ReceivedMessageContext context, IQueue controlQueue)
        {            
            Task[] deletes = new Task[context.MessageDataBatch.Count];
            for (int i = 0; i < context.MessageDataBatch.Count; i++)
            {
                var queueMessage = context.MessageDataBatch[i].OriginalQueueMessage;
                TaskMessage taskMessage = context.MessageDataBatch[i].TaskMessage;
                AnalyticsEventSource.Log.DeletingMessage(
                    this.queueManager.StorageName,
                    this.settings.TaskHubName,
                    taskMessage.Event.EventType.ToString(),
                    queueMessage.Id,
                    context.Instance.InstanceId,
                    context.Instance.ExecutionId);
                Task deletetask = controlQueue.DeleteMessageAsync(
                    queueMessage,
                    ((StorageOrchestrationServiceSettings)this.settings).ControlQueueRequestOptions,
                    context.StorageOperationContext);

                // Handle the case where this message was already deleted.
                deletes[i] = this.HandleNotFoundException(
                    deletetask,
                    queueMessage.Id,
                    context.Instance.InstanceId);
            }

            try
            {
                await Task.WhenAll(deletes);
            }
            finally
            {
                this.stats.StorageRequests.Increment(context.MessageDataBatch.Count);
            }
        }



        // REVIEW: There doesn't seem to be any code which calls this method.
        //         https://github.com/Azure/durabletask/issues/112
        /// <inheritdoc />
        public async Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem, out context))
            {
                // The context doesn't exist - possibly because this is a duplicate message.
                workItem.LockedUntilUtc = DateTime.UtcNow;
                return;
            }

            string instanceId = workItem.InstanceId;
            //var controlQueue = await this.queueManager.GetControlQueueAsync(instanceId);
            var controlQueue = this.queueManager.GetControlQueue(instanceId);

            // Reset the visibility of the message to ensure it doesn't get picked up by anyone else.
            try
            {
                await Task.WhenAll(context.MessageDataBatch.Select(e =>
                {
                    Task updateTask = controlQueue.UpdateMessageAsync(
                        e.OriginalQueueMessage,
                        this.settings.ControlQueueVisibilityTimeout,
                        MessageUpdateFields.Visibility,
                        ((StorageOrchestrationServiceSettings)this.settings).ControlQueueRequestOptions,
                        context.StorageOperationContext);

                    return this.HandleNotFoundException(updateTask, e.OriginalQueueMessage.Id, workItem.InstanceId);
                }));

                workItem.LockedUntilUtc = DateTime.UtcNow.Add(this.settings.ControlQueueVisibilityTimeout);
                this.stats.MessagesUpdated.Increment(context.MessageDataBatch.Count);
            }
            finally
            {
                this.stats.StorageRequests.Increment(context.MessageDataBatch.Count);
            }
        }

        /// <inheritdoc />
        public async Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem, out context))
            {
                // The context doesn't exist - possibly because this is a duplicate message.
                return;
            }

            string instanceId = workItem.InstanceId;
            //var controlQueue = await this.queueManager.GetControlQueueAsync(instanceId);
            var controlQueue = this.queueManager.GetControlQueue(instanceId);

            Task[] updates = new Task[context.MessageDataBatch.Count];

            // We "abandon" the message by settings its visibility timeout to zero.
            // This allows it to be reprocessed on this node or another node.
            for (int i = 0; i < context.MessageDataBatch.Count; i++)
            {
                var queueMessage = context.MessageDataBatch[i].OriginalQueueMessage;
                TaskMessage taskMessage = context.MessageDataBatch[i].TaskMessage;

                AnalyticsEventSource.Log.AbandoningMessage(
                    this.queueManager.StorageName,
                    this.settings.TaskHubName,
                    taskMessage.Event.EventType.ToString(),
                    queueMessage.Id,
                    taskMessage.OrchestrationInstance.InstanceId,
                    taskMessage.OrchestrationInstance.ExecutionId);

                Task abandonTask = controlQueue.UpdateMessageAsync(
                    queueMessage,
                    TimeSpan.Zero,
                    MessageUpdateFields.Visibility,
                    ((StorageOrchestrationServiceSettings)this.settings).ControlQueueRequestOptions,
                    context.StorageOperationContext);

                // Message may have been processed and deleted already.
                updates[i] = HandleNotFoundException(abandonTask, queueMessage.Id, instanceId);
            }

            try
            {
                await Task.WhenAll(updates);
            }
            finally
            {
                this.stats.StorageRequests.Increment(context.MessageDataBatch.Count);
            }
        }

        // Called after an orchestration completes an execution episode and after all messages have been enqueued.
        // Also called after an orchestration work item is abandoned.
        /// <inheritdoc />
        public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            // Release is local/in-memory only because instances are affinitized to queues and this
            // node already holds the lease for the target control queue.
            ReceivedMessageContext.RemoveContext(workItem);
            this.activeOrchestrationInstances.TryRemove(workItem.InstanceId, out _);
            return Utils.CompletedTask;
        }
        #endregion

        #region Task Activity Methods
        /// <inheritdoc />
        public async Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            await this.EnsuredCreatedIfNotExistsAsync();

            Stopwatch receiveTimeoutStopwatch = Stopwatch.StartNew();
            ReceivedMessageContext context;
            while (true)
            {
                context = await this.queueManager.GetMessageAsync(
                    this.WorkItemQueue,
                    this.settings.WorkItemQueueVisibilityTimeout,
                    ((StorageOrchestrationServiceSettings)this.settings).WorkItemQueueRequestOptions,
                    null /* operationContext */,
                    cancellationToken);
                this.stats.StorageRequests.Increment();

                if (context != null)
                {
                    break;
                }

                // If the Queue is empty, wait a bit to avoid hitting the Stored Procedure and consuming RUs when there is no work to be done
                // The previous If gets hit always as the queries always return before the receiveTimeout
                //if (receiveTimeoutStopwatch.Elapsed > receiveTimeout)
                //{
                //    return null;
                //}

                await this.workItemQueueBackoff.WaitAsync(cancellationToken);
            }

            this.stats.MessagesRead.Increment();
            this.workItemQueueBackoff.Reset();

            
            if (!context.TrySave(context.MessageId))
            {
                // This means we're already processing this message. This is never expected since the message
                // should be kept invisible via background calls to RenewTaskActivityWorkItemLockAsync.
                AnalyticsEventSource.Log.AssertFailure(
                    this.queueManager.StorageName,
                    this.settings.TaskHubName,
                    $"Work item queue message with ID = {context.MessageId} is being processed multiple times concurrently.");
                return null;
            }

            this.stats.ActiveActivityExecutions.Increment();

            return new TaskActivityWorkItem
            {
                Id = context.MessageId,
                TaskMessage = context.MessageData.TaskMessage,
                LockedUntilUtc = context.GetNextMessageExpirationTimeUtc(),
            };
        }

        /// <inheritdoc />
        public async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseTaskMessage)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem.Id, out context))
            {
                // The context does not exist - possibly because it was already removed.
                AnalyticsEventSource.Log.AssertFailure(
                    this.queueManager.StorageName,
                    this.settings.TaskHubName,
                    $"Could not find context for work item with ID = {workItem.Id}.");
                return;
            }

            string instanceId = workItem.TaskMessage.OrchestrationInstance.InstanceId;
            var controlQueue = await this.GetControlQueueAsync(instanceId);

            // First, send a response message back. If this fails, we'll try again later since we haven't deleted the
            // work item message yet (that happens next).
            try
            {
                await queueManager.AddMessageAsync(
                    controlQueue,
                    responseTaskMessage,
                    null /* timeToLive */,
                    null /* initialVisibilityDelay */,
                    ((StorageOrchestrationServiceSettings)this.settings).WorkItemQueueRequestOptions,
                    context.StorageOperationContext
                );

                this.stats.MessagesSent.Increment();
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            // Signal the control queue listener thread to poll immediately 
            // to avoid unnecessary delay between sending and receiving.
            this.controlQueueBackoff.Reset();

            string messageId = context.MessageData.OriginalQueueMessage.Id;

            // Next, delete the work item queue message. This must come after enqueuing the response message.
            AnalyticsEventSource.Log.DeletingMessage(
                this.queueManager.StorageName,
                this.settings.TaskHubName,
                workItem.TaskMessage.Event.EventType.ToString(),
                messageId,
                instanceId,
                context.Instance.ExecutionId);

            Task deleteTask = this.WorkItemQueue.DeleteMessageAsync(
                context.MessageData.OriginalQueueMessage,
                ((StorageOrchestrationServiceSettings)this.settings).WorkItemQueueRequestOptions,
                context.StorageOperationContext);

            try
            {
                // Handle the case where the message was already deleted
                await this.HandleNotFoundException(deleteTask, messageId, instanceId);
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            if (ReceivedMessageContext.RemoveContext(workItem.Id))
            {
                this.stats.ActiveActivityExecutions.Decrement();
            }
        }

        /// <inheritdoc />
        public async Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem.Id, out context))
            {
                // The context does not exist - possibly because it was already removed.
                // Expire the work item to prevent subsequent renewal attempts.
                return ExpireWorkItem(workItem);
            }

            string messageId = context.MessageData.OriginalQueueMessage.Id;
            string instanceId = workItem.TaskMessage.OrchestrationInstance.InstanceId;

            // Reset the visibility of the message to ensure it doesn't get picked up by anyone else.
            Task renewTask = this.WorkItemQueue.UpdateMessageAsync(
                context.MessageData.OriginalQueueMessage,
                this.settings.WorkItemQueueVisibilityTimeout,
                MessageUpdateFields.Visibility,
                ((StorageOrchestrationServiceSettings)this.settings).WorkItemQueueRequestOptions,
                context.StorageOperationContext);

            try
            {
                await this.HandleNotFoundException(renewTask, messageId, instanceId);
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            workItem.LockedUntilUtc = DateTime.UtcNow.Add(this.settings.WorkItemQueueVisibilityTimeout);
            this.stats.MessagesUpdated.Increment();

            return workItem;
        }

        static TaskActivityWorkItem ExpireWorkItem(TaskActivityWorkItem workItem)
        {
            workItem.LockedUntilUtc = DateTime.UtcNow;
            return workItem;
        }

        /// <inheritdoc />
        public async Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem.Id, out context))
            {
                // The context does not exist - possibly because it was already removed.
                AnalyticsEventSource.Log.AssertFailure(
                    this.queueManager.StorageName,
                    this.settings.TaskHubName,
                    $"Could not find context for work item with ID = {workItem.Id}.");
                return;
            }

            string messageId = context.MessageData.OriginalQueueMessage.Id;
            string instanceId = workItem.TaskMessage.OrchestrationInstance.InstanceId;

            AnalyticsEventSource.Log.AbandoningMessage(
                this.queueManager.StorageName,
                this.settings.TaskHubName,
                workItem.TaskMessage.Event.EventType.ToString(),
                messageId,
                instanceId,
                workItem.TaskMessage.OrchestrationInstance.ExecutionId);

            // We "abandon" the message by settings its visibility timeout to zero.
            Task abandonTask = this.WorkItemQueue.UpdateMessageAsync(
                context.MessageData.OriginalQueueMessage,
                TimeSpan.Zero,
                MessageUpdateFields.Visibility,
                ((StorageOrchestrationServiceSettings)this.settings).WorkItemQueueRequestOptions,
                context.StorageOperationContext);

            try
            {
                await this.HandleNotFoundException(abandonTask, messageId, instanceId);
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            if (ReceivedMessageContext.RemoveContext(workItem.Id))
            {
                this.stats.ActiveActivityExecutions.Decrement();
            }
        }

        Task HandleNotFoundException(Task storagetask, string messageId, string instanceId)
        {
            return storagetask.ContinueWith(t =>
            {
                StorageException e = t.Exception?.InnerException as StorageException;
                if (e?.RequestInformation?.HttpStatusCode == 404)
                {
                    // Message may have been processed and deleted already.
                    AnalyticsEventSource.Log.MessageGone(
                        this.queueManager.StorageName,
                        this.settings.TaskHubName,
                        messageId,
                        instanceId,
                        nameof(AbandonTaskOrchestrationWorkItemAsync));
                }
                else if (t.Exception?.InnerException != null)
                {
                    // Rethrow the original exception, preserving the callstack.
                    ExceptionDispatchInfo.Capture(t.Exception.InnerException).Throw();
                }
            });
        }
        #endregion

        /// <inheritdoc />
        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            // This orchestration service implementation will manage batch sizes by itself.
            // We don't want to rely on the underlying framework's backoff mechanism because
            // it would require us to implement some kind of duplicate message detection.
            return false;
        }

        /// <inheritdoc />
        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            // TODO: Need to reason about exception delays
            return 10;
        }

        /// <inheritdoc />
        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            // TODO: Need to reason about exception delays
            return 10;
        }
        #endregion

        #region IOrchestrationServiceClient
        /// <summary>
        /// Creates and starts a new orchestration.
        /// </summary>
        /// <param name="creationMessage">The message which creates and starts the orchestration.</param>
        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            return this.SendTaskOrchestrationMessageAsync(creationMessage);
        }

        /// <summary>
        /// Sends a list of messages to an orchestration.
        /// </summary>
        /// <remarks>
        /// Azure Storage does not support batch sending to queues, so there are no transactional guarantees in this method.
        /// </remarks>
        /// <param name="messages">The list of messages to send.</param>
        public Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            return Task.WhenAll(messages.Select(msg => this.SendTaskOrchestrationMessageAsync(msg)));
        }

        /// <summary>
        /// Sends a message to an orchestration.
        /// </summary>
        /// <param name="message">The message to send.</param>
        public async Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            // Client operations will auto-create the task hub if it doesn't already exist.
            await this.EnsuredCreatedIfNotExistsAsync();

            var controlQueue = await this.GetControlQueueAsync(message.OrchestrationInstance.InstanceId);

            await this.SendTaskOrchestrationMessageInternalAsync(controlQueue, message);

            ExecutionStartedEvent executionStartedEvent = message.Event as ExecutionStartedEvent;
            if (executionStartedEvent == null)
            {
                return;
            }

            await this.trackingStore.SetNewExecutionAsync(executionStartedEvent);

        }

        async Task SendTaskOrchestrationMessageInternalAsync(IQueue controlQueue, TaskMessage message)
        {
            await this.queueManager.AddMessageAsync(
                controlQueue, 
                message,                 
                null /* timeToLive */,
                null /* initialVisibilityDelay */,
                ((StorageOrchestrationServiceSettings)this.settings).ControlQueueRequestOptions,
                null /* operationContext */
                );

            this.stats.MessagesSent.Increment();

            // Notify the control queue poller that there are new messages to process.
            // TODO: This should be specific to the one control queue
            this.controlQueueBackoff.Reset();


        }

        /// <summary>
        /// Get the most current execution (generation) of the specified instance.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <param name="allExecutions">This parameter is not used.</param>
        /// <returns>List of <see cref="OrchestrationState"/> objects that represent the list of orchestrations.</returns>
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            // Client operations will auto-create the task hub if it doesn't already exist.
            await this.EnsuredCreatedIfNotExistsAsync();
            return await this.trackingStore.GetStateAsync(instanceId, allExecutions);
        }

        /// <summary>
        /// Get a the state of the specified execution (generation) of the specified orchestration instance.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <param name="executionId">The execution ID (generation) of the specified instance.</param>
        /// <returns>The <see cref="OrchestrationState"/> object that represents the orchestration.</returns>
        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            // Client operations will auto-create the task hub if it doesn't already exist.
            await this.EnsuredCreatedIfNotExistsAsync();
            return await this.trackingStore.GetStateAsync(instanceId, executionId);
        }




        /// <summary>
        /// Force terminates an orchestration by sending a execution terminated event
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration to terminate.</param>
        /// <param name="reason">The user-friendly reason for terminating.</param>
        public Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason)
        {
            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                Event = new ExecutionTerminatedEvent(-1, reason)
            };

            return SendTaskOrchestrationMessageAsync(taskMessage);
        }

        /// <summary>
        /// Get a string dump of the execution history of the specified execution (generation) of the specified instance.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <param name="executionId">The execution ID (generation) of the specified instance.</param>
        /// <returns>String with formatted JSON array representing the execution history.</returns>
        public async Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            OrchestrationRuntimeState runtimeState = await this.GetOrchestrationRuntimeStateAsync(
                instanceId,
                executionId);
            return JsonConvert.SerializeObject(runtimeState.Events);
        }

        /// <summary>
        /// Wait for an orchestration to reach any terminal state within the given timeout
        /// </summary>
        /// <param name="instanceId">The orchestration instance to wait for.</param>
        /// <param name="executionId">The execution ID (generation) of the specified instance.</param>
        /// <param name="timeout">Max timeout to wait.</param>
        /// <param name="cancellationToken">Task cancellation token.</param>
        public async Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException(nameof(instanceId));
            }

            TimeSpan statusPollingInterval = TimeSpan.FromSeconds(2);
            while (!cancellationToken.IsCancellationRequested && timeout > TimeSpan.Zero)
            {
                OrchestrationState state = await this.GetOrchestrationStateAsync(instanceId, executionId);
                if (state == null ||
                    state.OrchestrationStatus == OrchestrationStatus.Running ||
                    state.OrchestrationStatus == OrchestrationStatus.Pending)
                {
                    await Task.Delay(statusPollingInterval, cancellationToken);
                    timeout -= statusPollingInterval;
                }
                else
                {
                    return state;
                }
            }

            return null;
        }

        /// <summary>
        /// Purges orchestration instance state and history for orchestrations older than the specified threshold time.
        /// Also purges the blob storage. Currently only supported if a custom Instance store is provided.
        /// </summary>
        /// <param name="thresholdDateTimeUtc">Threshold date time in UTC</param>
        /// <param name="timeRangeFilterType">What to compare the threshold date time against</param>
        public Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            return this.trackingStore.PurgeHistoryAsync(thresholdDateTimeUtc, timeRangeFilterType);
        }

        #endregion

        // TODO: Change this to a sticky assignment so that partition count changes can
        //       be supported: https://github.com/Azure/azure-functions-durable-extension/issues/1
        async Task<IQueue> GetControlQueueAsync(string instanceId)
        {
            uint partitionIndex = Fnv1aHashHelper.ComputeHash(instanceId) % (uint)this.settings.PartitionCount;
            var controlQueue = this.queueManager.GetControlQueue(Utils.GetControlQueueId(settings.TaskHubName, (int)partitionIndex));

            IQueue cachedQueue;
            if (this.queueManager.OwnedControlQueues.TryGetValue(controlQueue.Name, out cachedQueue) ||
                this.queueManager.AllControlQueues.TryGetValue(controlQueue.Name, out cachedQueue))
            {
                return cachedQueue;
            }
            else
            {
                try
                {
                    await controlQueue.CreateIfNotExistsAsync();
                }
                finally
                {
                    this.stats.StorageRequests.Increment();
                }

                this.queueManager.AllControlQueues.TryAdd(controlQueue.Name, controlQueue);
                return controlQueue;
            }
        }

        class PendingMessageBatch
        {
            public string OrchestrationInstanceId { get; set; }
            public string OrchestrationExecutionId { get; set; }

            public List<MessageData> Messages { get; set; } = new List<MessageData>();
        }

        class ResettableLazy<T>
        {
            readonly Func<T> valueFactory;
            readonly LazyThreadSafetyMode threadSafetyMode;

            Lazy<T> lazy;

            public ResettableLazy(Func<T> valueFactory, LazyThreadSafetyMode mode)
            {
                this.valueFactory = valueFactory;
                this.threadSafetyMode = mode;

                this.Reset();
            }

            public T Value => this.lazy.Value;

            public void Reset()
            {
                this.lazy = new Lazy<T>(this.valueFactory, this.threadSafetyMode);
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

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
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        
        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
