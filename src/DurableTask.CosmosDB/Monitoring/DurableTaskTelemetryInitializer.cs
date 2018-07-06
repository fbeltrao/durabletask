using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.Extensibility;
using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.CosmosDB.Monitoring
{
    class DurableTaskTelemetryInitializer : ITelemetryInitializer
    {
        public DurableTaskTelemetryInitializer(string backendType)
        {
            BackendType = backendType;
        }

        public string BackendType { get; }

        public void Initialize(ITelemetry telemetry)
        {
            telemetry.Context.Properties.Add(nameof(BackendType), BackendType);
        }
    }
}
