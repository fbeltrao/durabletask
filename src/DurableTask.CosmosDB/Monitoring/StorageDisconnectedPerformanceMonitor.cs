﻿//  ----------------------------------------------------------------------------------
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

namespace DurableTask.CosmosDB.Monitoring
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.CosmosDB.Queue;
    using Microsoft.WindowsAzure.Storage;

    /// <summary>
    /// Utility class for collecting performance information for a Durable Task hub without actually running inside a Durable Task worker.
    /// </summary>
    public class StorageDisconnectedPerformanceMonitor : IDisconnectedPerformanceMonitor
    {
        internal const int QueueLengthSampleSize = 5;
        internal const int MaxMessagesPerWorkerRatio = 100;

        static readonly int MaxPollingLatency = (int)ExtensibleOrchestrationService.MaxQueuePollingDelay.TotalMilliseconds;
        static readonly int HighLatencyThreshold = Math.Min(MaxPollingLatency, 1000); // milliseconds
        const int LowLatencyThreshold = 200; // milliseconds
        static readonly Random Random = new Random();
        readonly string taskHub;

        int currentPartitionCount;
        int currentWorkItemQueueLength;
        int[] currentControlQueueLengths;

        readonly ExtensibleOrchestrationService service;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="service"></param>
        /// <param name="taskHub"></param>
        public StorageDisconnectedPerformanceMonitor(ExtensibleOrchestrationService service, string taskHub)
        {
            this.service = service;
            this.taskHub = taskHub;

        }

        internal virtual int PartitionCount => this.currentPartitionCount;

        internal List<QueueMetricHistory> ControlQueueLatencies { get; } = new List<QueueMetricHistory>();
        internal QueueMetricHistory WorkItemQueueLatencies { get; } = new QueueMetricHistory(QueueLengthSampleSize);

        /// <summary>
        /// Collects and returns a sampling of all performance metrics being observed by this instance.
        /// </summary>
        /// <param name="currentWorkerCount">The number of workers known to be processing messages for this task hub.</param>
        /// <returns>Returns a performance data summary or <c>null</c> if data cannot be obtained.</returns>
        public virtual async Task<PerformanceHeartbeat> PulseAsync(int currentWorkerCount)
        {
            if (!await this.UpdateQueueMetrics())
            {
                return null;
            }

            var heartbeatPayload = new PerformanceHeartbeat
            {
                PartitionCount = this.PartitionCount,
                WorkItemQueueLatency = TimeSpan.FromMilliseconds(this.WorkItemQueueLatencies.Latest),
                WorkItemQueueLength = this.currentWorkItemQueueLength,
                WorkItemQueueLatencyTrend = this.WorkItemQueueLatencies.CurrentTrend,
                ControlQueueLengths = this.currentControlQueueLengths,
                ControlQueueLatencies = this.ControlQueueLatencies.Select(h => TimeSpan.FromMilliseconds(h.Latest)).ToList(),
                ScaleRecommendation = this.MakeScaleRecommendation(currentWorkerCount),
            };

            return heartbeatPayload;
        }

        internal virtual async Task<bool> UpdateQueueMetrics()
        {

            var workItemQueue = service.WorkItemQueue;
            
            
            var controlQueues = await service.queueManager.GetControlQueuesAsync(partitionCount: Utils.DefaultPartitionCount);

            Task<QueueMetric> workItemMetricTask = GetQueueMetricsAsync(workItemQueue);
            List<Task<QueueMetric>> controlQueueMetricTasks = controlQueues.Select(GetQueueMetricsAsync).ToList();

            var tasks = new List<Task>(controlQueueMetricTasks.Count + 1);
            tasks.Add(workItemMetricTask);
            tasks.AddRange(controlQueueMetricTasks);

            try
            {
                await Task.WhenAll(tasks);
            }
            catch (StorageException e) when (e.RequestInformation?.HttpStatusCode == 404)
            {
                // The queues are not yet provisioned.
                AnalyticsEventSource.Log.MonitorWarning(
                    this.service.settings.StorageConnectionString,
                    this.taskHub,
                    $"Task hub has not been provisioned: {e.RequestInformation.ExtendedErrorInformation?.ErrorMessage}");
                return false;
            }

            QueueMetric workItemQueueMetric = workItemMetricTask.Result;
            this.WorkItemQueueLatencies.Add((int)workItemQueueMetric.Latency.TotalMilliseconds);

            int i;
            for (i = 0; i < controlQueueMetricTasks.Count; i++)
            {
                QueueMetric controlQueueMetric = controlQueueMetricTasks[i].Result;
                if (i >= this.ControlQueueLatencies.Count)
                {
                    this.ControlQueueLatencies.Add(new QueueMetricHistory(QueueLengthSampleSize));
                }

                this.ControlQueueLatencies[i].Add((int)controlQueueMetric.Latency.TotalMilliseconds);
            }

            // Handle the case where the number of control queues has been reduced since we last checked.
            while (i < this.ControlQueueLatencies.Count && this.ControlQueueLatencies.Count > 0)
            {
                this.ControlQueueLatencies.RemoveAt(this.ControlQueueLatencies.Count - 1);
            }

            this.currentPartitionCount = controlQueues.Length;
            this.currentWorkItemQueueLength = workItemQueueMetric.Length;
            this.currentControlQueueLengths = controlQueueMetricTasks.Select(t => t.Result.Length).ToArray();

            return true;
        }

        async Task<QueueMetric> GetQueueMetricsAsync(IQueue queue)
        {
            Task<TimeSpan> latencyTask = GetQueueLatencyAsync(queue);
            Task<int> lengthTask = GetQueueLengthAsync(queue);
            await Task.WhenAll(latencyTask, lengthTask);

            TimeSpan latency = latencyTask.Result;
            int length = lengthTask.Result;

            if (latency == TimeSpan.MinValue)
            {
                // No available queue messages (peek returned null)
                latency = TimeSpan.Zero;
                length = 0;
            }

            return new QueueMetric { Latency = latency, Length = length };
        }

        static async Task<TimeSpan> GetQueueLatencyAsync(IQueue queue)
        {
            DateTimeOffset now = DateTimeOffset.UtcNow;
            var firstMessage = await queue.PeekMessageAsync();
            if (firstMessage == null)
            {
                return TimeSpan.MinValue;
            }

            // Make sure we always return a non-negative timespan in the success case.
            TimeSpan latency = now.Subtract(firstMessage.InsertionTime.GetValueOrDefault(now));
            return latency < TimeSpan.Zero ? TimeSpan.Zero : latency;
        }

        static async Task<int> GetQueueLengthAsync(IQueue queue)
        {
            return await queue.GetQueueLenghtAsync();
        }

        struct QueueMetric
        {
            public TimeSpan Latency { get; set; }
            public int Length { get; set; }
        }

        /// <summary>
        /// Gets the scale-related status of the work-item queue.
        /// </summary>
        /// <returns>The approximate number of messages in the work-item queue.</returns>
        protected virtual async Task<WorkItemQueueData> GetWorkItemQueueStatusAsync()
        {
            var workItemQueue = service.WorkItemQueue;

            DateTimeOffset now = DateTimeOffset.Now;

            var fetchTask = workItemQueue.GetQueueLenghtAsync();
            var peekTask = workItemQueue.PeekMessageAsync();
            await Task.WhenAll(fetchTask, peekTask);

            int queueLength = fetchTask.Result;
            TimeSpan age = now.Subtract((peekTask.Result?.InsertionTime).GetValueOrDefault(now));
            if (age < TimeSpan.Zero)
            {
                age = TimeSpan.Zero;
            }

            return new WorkItemQueueData
            {
                QueueLength = queueLength,
                FirstMessageAge = age,
            };
        }

        /// <summary>
        /// Gets the approximate aggreate length (sum) of the all known control queues.
        /// </summary>
        /// <returns>The approximate number of messages across all control queues.</returns>
        protected virtual async Task<ControlQueueData> GetAggregateControlQueueLengthAsync()
        {
            var controlQueues = await service.queueManager.GetControlQueuesAsync(
                partitionCount: Utils.DefaultPartitionCount);

            // There is one queue per partition.
            var result = new ControlQueueData();
            result.PartitionCount = controlQueues.Length;

            // We treat all control queues like one big queue and sum the lengths together.
            foreach (var queue in controlQueues)
            {
                int queueLength = await queue.GetQueueLenghtAsync();
                result.AggregateQueueLength += queueLength;
            }

            return result;
        }

        ScaleRecommendation MakeScaleRecommendation(int workerCount)
        {
            // REVIEW: Is zero latency a reliable indicator of idle?
            bool taskHubIsIdle = IsIdle(this.WorkItemQueueLatencies) && this.ControlQueueLatencies.TrueForAll(IsIdle);
            if (workerCount == 0 && !taskHubIsIdle)
            {
                return new ScaleRecommendation(ScaleAction.AddWorker, keepWorkersAlive: true, reason: "First worker");
            }

            // Wait until we have enough samples before making specific recommendations
            if (!this.WorkItemQueueLatencies.IsFull || !this.ControlQueueLatencies.TrueForAll(h => h.IsFull))
            {
                return new ScaleRecommendation(ScaleAction.None, keepWorkersAlive: !taskHubIsIdle, reason: "Not enough samples");
            }

            if (taskHubIsIdle)
            {
                return new ScaleRecommendation(
                    scaleAction: workerCount > 0 ? ScaleAction.RemoveWorker : ScaleAction.None,
                    keepWorkersAlive: false,
                    reason: "Task hub is idle");
            }
            else if (IsHighLatency(this.WorkItemQueueLatencies))
            {
                return new ScaleRecommendation(
                    ScaleAction.AddWorker,
                    keepWorkersAlive: true,
                    reason: $"Work-item queue latency: {this.WorkItemQueueLatencies.Latest} > {HighLatencyThreshold}");
            }
            else if (workerCount > this.PartitionCount && IsIdle(this.WorkItemQueueLatencies))
            {
                return new ScaleRecommendation(
                    ScaleAction.RemoveWorker,
                    keepWorkersAlive: true,
                    reason: $"Work-items idle, #workers > partitions ({workerCount} > {this.PartitionCount})");
            }

            // Control queues are partitioned; only scale-out if there are more partitions than workers.
            if (workerCount < this.ControlQueueLatencies.Count(IsHighLatency))
            {
                // Some control queues are busy, so scale out until workerCount == partitionCount.
                QueueMetricHistory metric = this.ControlQueueLatencies.First(IsHighLatency);
                return new ScaleRecommendation(
                    ScaleAction.AddWorker,
                    keepWorkersAlive: true,
                    reason: $"High control queue latency: {metric.Latest} > {HighLatencyThreshold}");
            }
            else if (workerCount > this.ControlQueueLatencies.Count(h => !IsIdle(h)) && IsIdle(this.WorkItemQueueLatencies))
            {
                // If the work item queues are idle, scale down to the number of non-idle control queues.
                return new ScaleRecommendation(
                    ScaleAction.RemoveWorker,
                    keepWorkersAlive: this.ControlQueueLatencies.Any(IsIdle),
                    reason: $"One or more control queues idle");
            }
            else if (workerCount > 1)
            {
                // If all queues are operating efficiently, it can be hard to know if we need to reduce the worker count.
                // We want to avoid the case where a constant trickle of load after a big scale-out prevents scaling back in.
                // We also want to avoid scaling in unnecessarily when we've reached optimal scale-out. To balance these
                // goals, we check for low latencies and vote to scale down 10% of the time when we see this. The thought is
                // that it's a slow scale-in that will get automatically corrected once latencies start increasing again.
                bool tryRandomScaleDown = Random.Next(10) == 0;
                if (tryRandomScaleDown &&
                    this.ControlQueueLatencies.TrueForAll(IsLowLatency) &&
                    this.WorkItemQueueLatencies.TrueForAll(latency => latency < LowLatencyThreshold))
                {
                    return new ScaleRecommendation(
                        ScaleAction.RemoveWorker,
                        keepWorkersAlive: true,
                        reason: $"All queues are not busy");
                }
            }

            // Load exists, but none of our scale filters were triggered, so we assume that the current worker
            // assignments are close to ideal for the current workload.
            return new ScaleRecommendation(ScaleAction.None, keepWorkersAlive: true, reason: $"Queue latencies are healthy");
        }

        static bool IsHighLatency(QueueMetricHistory history)
        {
            if (history.Previous == 0)
            {
                // If previous was zero, the queue may have been idle, which means
                // backoff polling might have been the reason for the latency.
                return history.Latest >= MaxPollingLatency;
            }

            return history.Latest >= HighLatencyThreshold;
        }

        static bool IsLowLatency(QueueMetricHistory history)
        {
            return history.Latest <= LowLatencyThreshold && history.Previous <= LowLatencyThreshold;
        }

        static bool IsIdle(QueueMetricHistory history)
        {
            return history.IsAllZeros();
        }
    }
}
