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
        int latestValue;
        int previousValue;
        double? currentTrend;

        public QueueMetricHistory(int maxSize)
        {
            this.history = new int[maxSize];
        }

        public bool IsFull
        {
            get { return this.count == this.history.Length; }
        }

        public int Latest => this.latestValue;

        public int Previous => this.previousValue;

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

            this.previousValue = this.latestValue;
            this.latestValue = value;

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
