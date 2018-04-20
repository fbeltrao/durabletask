namespace CosmosDBQueue
{
    /// <summary>
    /// Settings for a CosmosDB queue producer
    /// </summary>
    public class CosmosDBQueueProducerSettings
    {
        /// <summary>
        /// Defines the collection where queue items are stored
        /// </summary>
        public CosmosDBCollectionDefinition QueueCollectionDefinition { get; set; }
    }
}
