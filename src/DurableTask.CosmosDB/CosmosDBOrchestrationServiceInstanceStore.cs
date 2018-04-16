//using DurableTask.Core;
//using DurableTask.Core.Tracking;
//using System;
//using System.Collections.Generic;
//using System.Text;
//using System.Threading.Tasks;

//namespace DurableTask.CosmosDB
//{
//    /// <summary>
//    /// <see cref="IOrchestrationServiceInstanceStore"/> implementatio using CosmosDB
//    /// </summary>
//    public class CosmosDBOrchestrationServiceInstanceStore : IOrchestrationServiceInstanceStore
//    {
//        const int MaxDisplayStringLengthForAzureTableColumn = (1024 * 24) - 20;
//        const int MaxRetriesTableStore = 5;
//        const int IntervalBetweenRetriesSecs = 5;

//        /// <summary>
//        /// Creates a new AzureTableInstanceStore using the supplied hub name and table connection string
//        /// </summary>
//        /// <param name="hubName">The hubname for this instance store</param>
//        /// <param name="tableConnectionString">Azure table connection string</param>
//        public CosmosDBOrchestrationServiceInstanceStore(string hubName, string cosmosDBConnectionString)
//        {
            

//            // Workaround an issue with Storage that throws exceptions for any date < 1600 so DateTime.Min cannot be used
//            DateTimeUtils.SetMinDateTimeForStorageEmulator();
//        }

//        /// <summary>
//        /// Gets the maximum length a history entry can be so it can be truncated if necessary
//        /// </summary>
//        /// <returns>The maximum length</returns>
//        public int MaxHistoryEntryLength => MaxDisplayStringLengthForAzureTableColumn

//        public Task<object> DeleteEntitiesAsync(IEnumerable<InstanceEntityBase> entities)
//        {
//            throw new NotImplementedException();
//        }

//        public Task<object> DeleteJumpStartEntitiesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities)
//        {
//            throw new NotImplementedException();
//        }

//        public Task DeleteStoreAsync()
//        {
//            throw new NotImplementedException();
//        }

//        public Task<IEnumerable<OrchestrationStateInstanceEntity>> GetEntitiesAsync(string instanceId, string executionId)
//        {
//            throw new NotImplementedException();
//        }

//        public Task<IEnumerable<OrchestrationJumpStartInstanceEntity>> GetJumpStartEntitiesAsync(int top)
//        {
//            throw new NotImplementedException();
//        }

//        public Task<IEnumerable<OrchestrationWorkItemInstanceEntity>> GetOrchestrationHistoryEventsAsync(string instanceId, string executionId)
//        {
//            throw new NotImplementedException();
//        }

//        public Task<IEnumerable<OrchestrationStateInstanceEntity>> GetOrchestrationStateAsync(string instanceId, bool allInstances)
//        {
//            throw new NotImplementedException();
//        }

//        public Task<OrchestrationStateInstanceEntity> GetOrchestrationStateAsync(string instanceId, string executionId)
//        {
//            throw new NotImplementedException();
//        }

//        public Task InitializeStoreAsync(bool recreate)
//        {
//            throw new NotImplementedException();
//        }

//        public Task<int> PurgeOrchestrationHistoryEventsAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
//        {
//            throw new NotImplementedException();
//        }

//        public Task<object> WriteEntitiesAsync(IEnumerable<InstanceEntityBase> entities)
//        {
//            throw new NotImplementedException();
//        }

//        public Task<object> WriteJumpStartEntitiesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities)
//        {
//            throw new NotImplementedException();
//        }
//    }
//}
