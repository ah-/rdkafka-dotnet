using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RdKafka.Internal;

namespace RdKafka
{
    /// <summary>
    /// High-level Kafka Consumer, receives messages from a Kafka cluster.
    ///
    /// Requires Kafka >= 0.9.0.0.
    /// </summary>
    public class Consumer : Handle, IDisposable
    {
        public Consumer(Config config, string brokerList = null)
        {
            RebalanceDelegate = RebalanceCallback;
            CommitDelegate = CommitCallback;

            IntPtr cfgPtr = config.handle.Dup();
            LibRdKafka.conf_set_rebalance_cb(cfgPtr, RebalanceDelegate);
            LibRdKafka.conf_set_offset_commit_cb(cfgPtr, CommitDelegate);
            if (config.DefaultTopicConfig != null)
            {
                LibRdKafka.conf_set_default_topic_conf(cfgPtr,
                        config.DefaultTopicConfig.handle.Dup());
            }
            Init(RdKafkaType.Consumer, cfgPtr, config.Logger);

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
            handle.Assign(null);
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
        /// </summary>
        public Task Commit()
        {
            handle.Commit();
            return Task.FromResult(false);
        }

        /// <summary>
        /// Commit offset for a single topic+partition based on message.
        /// </summary>
        public Task Commit(Message message)
        {
            Commit(new List<TopicPartitionOffset>() { message.TopicPartitionOffset });
            return Task.FromResult(false);
        }

        /// <summary>
        /// Commit explicit list of offsets.
        /// </summary>
        public Task Commit(List<TopicPartitionOffset> offsets)
        {
            handle.Commit(offsets);
            return Task.FromResult(false);
        }

        /// <summary>
        /// Retrieve committed offsets for topics+partitions.
        /// </summary>
        public Task<List<TopicPartitionOffset>> Committed(List<TopicPartition> partitions, TimeSpan timeout)
        {
            var result = handle.Committed(partitions, (IntPtr) timeout.TotalMilliseconds);
            return Task.FromResult(result);
        }

        /// <summary>
        /// Retrieve current positions (offsets) for topics+partitions.
        ///
        /// The offset field of each requested partition will be set to the offset
        /// of the last consumed message + 1, or RD_KAFKA_OFFSET_INVALID in case there was
        /// no previous message.
        /// </summary>
        public List<TopicPartitionOffset> Position(List<TopicPartition> partitions)
        {
            return handle.Position(partitions);
        }

        // Rebalance callbacks
        public event EventHandler<List<TopicPartitionOffset>> OnPartitionsAssigned;
        public event EventHandler<List<TopicPartitionOffset>> OnPartitionsRevoked;

        // Explicitly keep reference to delegate so it stays alive
        LibRdKafka.RebalanceCallback RebalanceDelegate;
        void RebalanceCallback(IntPtr rk, ErrorCode err,
                /* rd_kafka_topic_partition_list_t * */ IntPtr partitions,
                IntPtr opaque)
        {
            var partitionList = SafeKafkaHandle.GetTopicPartitionOffsetList(partitions);
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

        public struct OffsetCommitArgs
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
                        Offsets = SafeKafkaHandle.GetTopicPartitionOffsetList(offsets)
                    });
        }

        public override void Dispose()
        {
            handle.ConsumerClose();
            base.Dispose();
        }
    }
}
