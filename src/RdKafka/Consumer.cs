using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using RdKafka.Internal;

namespace RdKafka
{
    public class Consumer : IDisposable
    {
        readonly SafeKafkaHandle handle;
        Task consumerTask;
        CancellationTokenSource consumerCts;

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

        /// <summary>
        /// Returns the current partition assignment as set by Assign.
        /// </summary>
        public List<TopicPartition> Assignment => handle.GetAssignment();

        /// <summary>
        /// Returns the current partition subscription as set by Subscribe.
        /// </summary>
        public List<string> Subscription => handle.GetSubscription();

        /// <summary>
        /// Update the subscription set to topics.
        ///
        /// Any previous subscription will be unassigned and unsubscribed first.
        ///
        /// The subscription set denotes the desired topics to consume and this
        /// set is provided to the partition assignor (one of the elected group
        /// members) for all clients which then uses the configured
        /// partition.assignment.strategy to assign the subscription sets's
        /// topics's partitions to the consumers, depending on their subscription.
        /// </summary>
        public void Subscribe(List<string> topics)
        {
            handle.Subscribe(topics);
        }

        /// <summary>
        /// Unsubscribe from the current subscription set.
        /// </summary>
        public void Unsubscribe()
        {
            handle.Unsubscribe();
        }

        /// <summary>
        /// Update the assignment set to \p partitions.
        ///
        /// The assignment set is the set of partitions actually being consumed
        /// by the KafkaConsumer.
        /// </summary>
        public void Assign(List<TopicPartitionOffset> partitions)
        {
            handle.Assign(partitions);
        }

        /// <summary>
        /// Stop consumption and remove the current assignment.
        /// </summary>
        public void Unassign()
        {
            handle.Assign(new List<TopicPartitionOffset>());
        }

        /// <summary>
        /// Manually consume message or get error, triggers events.
        ///
        /// Will invoke events for OnPartitionsAssigned/Revoked,
        /// OnOffsetCommit, etc. on the calling thread.
        ///
        /// Returns one of:
        /// - proper message (ErrorCode is NO_ERROR)
        /// - error event (ErrorCode is != NO_ERROR)
        /// - timeout due to no message or event within timeout (null)
        /// </summary>
        public MessageAndError? Consume(TimeSpan timeout)
        {
            return handle.ConsumerPoll((IntPtr) timeout.TotalMilliseconds);
        }

        /// <summary>
        /// Commit offsets for the current assignment.
        ///
        /// This is the synchronous variant that blocks until offsets 
        /// are committed or the commit fails.
        /// </summary>
        public void Commit()
        {
            handle.Commit();
        }

        /// <summary>
        /// Commit offset for a single topic+partition based on message.
        ///
        /// This is the synchronous variant that blocks until offsets 
        /// are committed or the commit fails.
        /// </summary>
        public void Commit(Message message)
        {
            Commit(new List<TopicPartitionOffset>() { message.TopicPartitionOffset });
        }

        /// <summary>
        /// Commit explicit list of offsets.
        ///
        /// This is the synchronous variant that blocks until offsets 
        /// are committed or the commit fails.
        /// </summary>
        public void Commit(List<TopicPartitionOffset> offsets)
        {
            handle.Commit(offsets);
        }

        public string Name => handle.GetName();

        // Rebalance callbacks
        public event EventHandler<List<TopicPartitionOffset>> OnPartitionsAssigned;
        public event EventHandler<List<TopicPartitionOffset>> OnPartitionsRevoked;

        // Explicitly keep reference to delegate so it stays alive
        LibRdKafka.RebalanceCallback RebalanceDelegate;
        void RebalanceCallback(IntPtr rk, ErrorCode err,
                /* rd_kafka_topic_partition_list_t * */ IntPtr partitions,
                IntPtr opaque)
        {
            var partitionList = LibRdKafka.GetTopicPartitionOffsetList(partitions);
            if (err == ErrorCode._ASSIGN_PARTITIONS)
            {
                var handler = OnPartitionsAssigned;
                if (handler != null && handler.GetInvocationList().Length > 0)
                {
                    handler(this, partitionList);
                }
                else
                {
                    Assign(partitionList);
                }
            }
            if (err == ErrorCode._REVOKE_PARTITIONS)
            {
                var handler = OnPartitionsRevoked;
                if (handler != null && handler.GetInvocationList().Length > 0)
                {
                    handler(this, partitionList);
                }
                else
                {
                    Unassign();
                }
            }
        }

        public class OffsetCommitArgs
        {
            public ErrorCode Error { get; set; }
            public IList<TopicPartitionOffset> Offsets { get; set; }
        }
        public event EventHandler<OffsetCommitArgs> OnOffsetCommit;

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

        public event EventHandler<Message> OnMessage;
        public event EventHandler<TopicPartitionOffset> OnEndReached;

        /// <summary>
        /// Start automatically consuming message and trigger events.
        ///
        /// Will invoke events for OnMessage, OnEndReached,
        /// OnPartitionsAssigned/Revoked, OnOffsetCommit, etc.
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

        public void Dispose()
        {
            if (consumerTask != null)
            {
                Stop().Wait();
            }

            handle.ConsumerClose();
        }

        /*
        public string MemberId()
        {
            return handle.MemberId();
        }
        */
    }
}
