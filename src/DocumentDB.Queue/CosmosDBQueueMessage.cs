using Microsoft.Azure.Documents;
using System.Threading.Tasks;

namespace DocumentDB.Queue
{
    public class CosmosDBQueueMessage
    {
        private CosmosDBQueueMessageCompleter completer;

        public CosmosDBQueueMessage()
        {
            this.Data = new Document();
        }

        public CosmosDBQueueMessage(Document doc, CosmosDBQueueMessageCompleter completer)
        {
            this.Data = doc;
            this.completer = completer;
        }

        internal async Task Complete()
        {
            if (this.completer != null)
                await this.completer.Complete().ConfigureAwait(false);
        }

        public Document Data { get; set; }

        public string Id => this.Data?.Id ?? string.Empty;
    }
}
