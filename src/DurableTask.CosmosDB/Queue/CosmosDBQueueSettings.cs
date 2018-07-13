﻿namespace DurableTask.CosmosDB
{
    /// <summary>
    /// 
    /// </summary>
    public class CosmosDBQueueSettings
    {
        /// <summary>
        /// 
        /// </summary>
        public CosmosDBCollectionDefinition QueueCollectionDefinition { get; set; }
        
        /// <summary>
        /// 
        /// </summary>
        public bool UseOneCollectionPerQueueType { get; internal set; }
    }
}