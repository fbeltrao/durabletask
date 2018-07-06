using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace DurableTask.CosmosDB.Monitoring
{
    /// <summary>
    /// Helper to monitor telemetry
    /// </summary>
    public sealed class TelemetryRecorder : IDisposable
    {
        private readonly TelemetryClient client;
        private readonly string dependencyType;
        private readonly IDictionary<string, string> properties;
        private readonly Stopwatch startTime;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="client"></param>
        /// <param name="dependencyType"></param>
        /// <param name="properties"></param>
        public TelemetryRecorder(TelemetryClient client, string dependencyType, IDictionary<string, string> properties = null)
        {
            this.client = client;            
            this.dependencyType = dependencyType;
            this.properties = properties;
            if (this.client != null)
                this.startTime = Stopwatch.StartNew();
        }
        
        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {            
            GC.SuppressFinalize(this);

            if (client != null)
            {
                startTime.Stop();
                var telemetry = new DependencyTelemetry()
                {
                    Type = dependencyType,
                    Duration = startTime.Elapsed
                };
                
                if (properties?.Count > 0)
                {
                    foreach (var kv in properties)
                        telemetry.Properties[kv.Key] = kv.Value;
                }

                client.TrackDependency(telemetry);
            }
        }
    }
}
