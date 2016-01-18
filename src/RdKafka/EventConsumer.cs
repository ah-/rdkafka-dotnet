using System;
using System.Threading;
using System.Threading.Tasks;

namespace RdKafka
{
    public class EventConsumer : Consumer, IDisposable
    {
        Task consumerTask;
        CancellationTokenSource consumerCts;

        public event EventHandler<Message> OnMessage;
        public event EventHandler<ErrorCode> OnError;
        public event EventHandler<TopicPartitionOffset> OnEndReached;

        public EventConsumer(Config config, string brokerList = null)
            : base(config, brokerList)
        {}

        /// <summary>
        /// Start automatically consuming message and trigger events.
        ///
        /// Will invoke OnMessage, OnEndReached and OnError events.
        /// </summary>
        public void Start()
        {
            if (consumerTask != null)
            {
                throw new InvalidOperationException("Consumer task already running");
            }

            consumerCts = new CancellationTokenSource();
            var ct = consumerCts.Token;
            consumerTask = Task.Factory.StartNew(() =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        var messageAndError = Consume(TimeSpan.FromSeconds(1));
                        if (messageAndError.HasValue)
                        {
                            var mae = messageAndError.Value;
                            if (mae.Error == ErrorCode.NO_ERROR)
                            {
                                OnMessage?.Invoke(this, mae.Message);
                            }
                            else if (mae.Error == ErrorCode._PARTITION_EOF)
                            {
                                OnEndReached?.Invoke(this, 
                                        new TopicPartitionOffset()
                                        {
                                            Topic = mae.Message.Topic,
                                            Partition = mae.Message.Partition,
                                            Offset = mae.Message.Offset,
                                        });
                            }
                            else
                            {
                                OnError?.Invoke(this, mae.Error);
                            }
                        }
                    }
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public async Task Stop()
        {
            consumerCts.Cancel();
            try
            {
                await consumerTask;
            }
            finally
            {
                consumerTask = null;
                consumerCts = null;
            }
        }

        public override void Dispose()
        {
            if (consumerTask != null)
            {
                Stop().Wait();
            }

            base.Dispose();
        }
    }
}
