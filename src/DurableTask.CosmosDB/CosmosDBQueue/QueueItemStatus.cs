namespace CosmosDBQueue
{
    /// <summary>
    /// Queue item status
    /// </summary>
    public enum QueueItemStatus
    {
        /// <summary>
        /// Pending
        /// </summary>
        Pending, 

        /// <summary>
        /// Item is being processed
        /// </summary>
        InProgress,

        /// <summary>
        /// Item is completed
        /// </summary>
        Completed,

        /// <summary>
        /// Item failed (deadletter like)
        /// </summary>
        Failed

    }
}