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
        private readonly string dependencyName;
        private readonly string dependencyType;
        private IDictionary<string, string> properties;
        private readonly Stopwatch startTime;
        bool success = false;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dependencyType"></param>
        /// <param name="properties"></param>
        /// <param name="dependencyName"></param>
        public TelemetryRecorder(string dependencyName, string dependencyType, IDictionary<string, string> properties = null) : this(TelemetryClientProvider.TelemetryClient, dependencyName, dependencyType, properties)
        {

        }


        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="client"></param>
        /// <param name="dependencyType"></param>
        /// <param name="properties"></param>
        public TelemetryRecorder(TelemetryClient client, string dependencyType, IDictionary<string, string> properties = null) : this(client, null, dependencyType, properties)
        {            
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="client"></param>
        /// <param name="dependencyType"></param>
        /// <param name="properties"></param>
        /// <param name="dependencyName"></param>
        public TelemetryRecorder(TelemetryClient client, string dependencyName, string dependencyType, IDictionary<string, string> properties = null)
        {
            this.client = client;
            this.dependencyName = dependencyName;
            this.dependencyType = dependencyType;
            this.properties = properties;
            if (this.client != null)
                this.startTime = Stopwatch.StartNew();
        }
        

        internal TelemetryRecorder AsSucceeded()
        {
            this.success = true;
            return this;
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
                    Name = dependencyName,
                    Duration = startTime.Elapsed
                };
                
                if (properties?.Count > 0)
                {
                    foreach (var kv in properties)
                        telemetry.Properties[kv.Key] = kv.Value;
                }

                telemetry.Success = success;                

                client.TrackDependency(telemetry);
            }
        }

        internal TelemetryRecorder AddProperty(string name, string value)
        {
            properties = (properties ?? new Dictionary<string, string>());
            properties.Add(name, value);
            return this;
        }
    }
}
