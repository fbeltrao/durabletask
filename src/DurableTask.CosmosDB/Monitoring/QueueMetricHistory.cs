//  ----------------------------------------------------------------------------------
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

using System;
using System.Text;

namespace DurableTask.CosmosDB.Monitoring
{
    class QueueMetricHistory
    {
        const double TrendThreshold = 0.0;

        readonly int[] history;
        int next;
        int count;
        double? currentTrend;

        public QueueMetricHistory(int maxSize)
        {
            this.history = new int[maxSize];
        }

        public bool IsFull
        {
            get { return this.count == this.history.Length; }
        }

        public int Latest { get; private set; }

        public int Previous { get; private set; }

        public bool IsTrendingUpwards => this.CurrentTrend > TrendThreshold;

        public bool IsTrendingDownwards => this.CurrentTrend < -TrendThreshold;

        public double CurrentTrend
        {
            get
            {
                if (!this.IsFull)
                {
                    return 0.0;
                }

                if (!this.currentTrend.HasValue)
                {
                    int firstIndex = this.IsFull ? this.next : 0;
                    int first = this.history[firstIndex];
                    if (first == 0)
                    {
                        // discard trend information when the first item is a zero.
                        this.currentTrend = 0.0;
                    }
                    else
                    {
                        int sum = 0;
                        for (int i = 0; i < this.history.Length; i++)
                        {
                            sum += this.history[i];
                        }

                        double average = (double)sum / this.history.Length;
                        this.currentTrend = (average - first) / first;
                    }
                }

                return this.currentTrend.Value;
            }
        }

        public void Add(int value)
        {
            this.history[this.next++] = value;
            if (this.count < this.history.Length)
            {
                this.count++;
            }

            if (this.next >= this.history.Length)
            {
                this.next = 0;
            }

            this.Previous = this.Latest;
            this.Latest = value;

            // invalidate any existing trend information
            this.currentTrend = null;
        }

        public bool IsAllZeros()
        {
            return Array.TrueForAll(this.history, i => i == 0);
        }

        public bool TrueForAll(Predicate<int> predicate)
        {
            return Array.TrueForAll(this.history, predicate);
        }

        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.Append('[');

            for (int i = 0; i < this.history.Length; i++)
            {
                int index = (i + this.next) % this.history.Length;
                builder.Append(this.history[index]).Append(',');
            }

            builder.Remove(builder.Length - 1, 1).Append(']');
            return builder.ToString();
        }

        static void ThrowIfNegative(string paramName, double value)
        {
            if (value < 0.0)
            {
                throw new ArgumentOutOfRangeException(paramName, value, $"{paramName} cannot be negative.");
            }
        }

        static void ThrowIfPositive(string paramName, double value)
        {
            if (value > 0.0)
            {
                throw new ArgumentOutOfRangeException(paramName, value, $"{paramName} cannot be positive.");
            }
        }
    }
}
