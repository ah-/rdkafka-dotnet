using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using RdKafka.Internal;

namespace RdKafka
{
    public class Consumer
    {
        readonly SafeKafkaHandle handle;
        Task pollingTask;
        CancellationTokenSource pollingCts;

        public Consumer(Config config, string brokerList = null)
        {
            RebalanceDelegate = RebalanceCallback;
            LibRdKafka.rd_kafka_conf_set_rebalance_cb(config.handle.DangerousGetHandle(),
                    RebalanceDelegate);
            CommitDelegate = CommitCallback;
            LibRdKafka.rd_kafka_conf_set_offset_commit_cb(config.handle.DangerousGetHandle(),
                    CommitDelegate);
            handle = SafeKafkaHandle.Create(RdKafkaType.Consumer, config.handle);
            if (brokerList != null)
            {
                handle.AddBrokers(brokerList);
            }
        }

        public string Name => handle.GetName();
        public event EventHandler<Message> OnMessage;

        // Rebalance callbacks
        public event EventHandler<IList<TopicPartition>> OnPartitionsAssigned;
        public event EventHandler<IList<TopicPartition>> OnPartitionsRevoked;

        public event EventHandler<TopicPartitionOffset> OnEndReached;

        public class OffsetCommitArgs
        {
            public ErrorCode Error { get; set; }
            public IList<TopicPartitionOffset> Offsets { get; set; }
        }
        public event EventHandler<OffsetCommitArgs> OnOffsetCommit;

        // Explicitly keep reference to delegate so it stays alive
        LibRdKafka.RebalanceCallback RebalanceDelegate;
        void RebalanceCallback(IntPtr rk, ErrorCode err,
                /* rd_kafka_topic_partition_list_t * */ IntPtr partitions,
                IntPtr opaque)
        {
            if (err == ErrorCode._ASSIGN_PARTITIONS)
            {
                OnPartitionsAssigned?.Invoke(this,
                        LibRdKafka.GetTopicPartitionList(partitions));
                ErrorCode aerr = handle.Assign(partitions);
                if (aerr != ErrorCode.NO_ERROR) {
                    throw RdKafkaException.FromErr(aerr, "Failed to assign partitions");
                }
            }
            if (err == ErrorCode._REVOKE_PARTITIONS)
            {
                OnPartitionsRevoked?.Invoke(this,
                        LibRdKafka.GetTopicPartitionList(partitions));
                ErrorCode aerr = handle.Assign(IntPtr.Zero);
                if (aerr != ErrorCode.NO_ERROR) {
                    throw RdKafkaException.FromErr(aerr, "Failed to revoke partitions");
                }
            }
        }

        // Explicitly keep reference to delegate so it stays alive
        LibRdKafka.CommitCallback CommitDelegate;
        internal void CommitCallback(IntPtr rk,
                ErrorCode err,
                /* rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                IntPtr opaque)
        {
            OnOffsetCommit?.Invoke(this, new OffsetCommitArgs()
                    {
                        Error = err,
                        Offsets = LibRdKafka.GetTopicPartitionOffsetList(offsets)
                    });
        }

        public void Subscribe(params string[] topics)
        {
            handle.Subscribe(topics.ToList());
        }

        public void Subscribe(List<string> topics)
        {
            handle.Subscribe(topics);
        }

        public void Unsubscribe()
        {
            handle.Unsubscribe();
        }

        /*
        public void Unsubscribe(string topic, int partition)
        {
            handle.Unsubscribe(topic, partition);
        }
        */

        public void Start()
        {
            // TODO: check if already polling
            this.pollingCts = new CancellationTokenSource();
            var ct = this.pollingCts.Token;
            this.pollingTask = Task.Factory.StartNew(() =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        var messageAndError = handle.ConsumerPoll(
                                (IntPtr) TimeSpan.FromSeconds(1).TotalMilliseconds);
                        if (messageAndError.HasValue)
                        {
                            var mae = messageAndError.Value;
                            if (mae.Error == ErrorCode.NO_ERROR)
                            {
                                OnMessage?.Invoke(this, mae.Message);
                            }
                            if (mae.Error == ErrorCode._PARTITION_EOF)
                            {
                                OnEndReached?.Invoke(this, 
                                        new TopicPartitionOffset()
                                        {
                                            Topic = mae.Message.Topic,
                                            Partition = mae.Message.Partition,
                                            Offset = mae.Message.Offset,
                                        });
                            }
                        }
                    }
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public async Task Stop()
        {
            pollingCts.Cancel();
            try
            {
                await pollingTask;
            }
            finally
            {
                handle.ConsumerClose();
            }
        }

        public List<TopicPartition> GetSubscriptions() => handle.GetSubscriptions();

        public List<TopicPartition> GetAssignment() => handle.GetAssignment();

        public void Commit(List<TopicPartitionOffset> offsets)
        {
            handle.Commit(offsets);
        }

        public void Commit(Message message)
        {
            Commit(new List<TopicPartitionOffset>()
                    { message.TopicPartitionOffset });
        }

        /*
        public string MemberId()
        {
            return handle.MemberId();
        }
        */
    }
}
