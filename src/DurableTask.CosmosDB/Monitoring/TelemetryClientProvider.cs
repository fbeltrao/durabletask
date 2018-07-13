using Microsoft.ApplicationInsights;

namespace DurableTask.CosmosDB.Monitoring
{
    internal sealed class TelemetryClientProvider
    {
        static TelemetryClient _client;

        internal static TelemetryClient TelemetryClient { get { return _client; } }
        internal static void Set(TelemetryClient client)
        {
            _client = client;
        }
        

    }
}
