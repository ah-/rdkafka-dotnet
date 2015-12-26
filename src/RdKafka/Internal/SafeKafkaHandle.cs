using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;

namespace RdKafka.Internal
{
    enum RdKafkaType
    {
        Producer,
        Consumer
    }

    internal sealed class SafeKafkaHandle : SafeHandleZeroIsInvalid
    {
        const int RD_KAFKA_PARTITION_UA = -1;

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern SafeKafkaHandle rd_kafka_new(
                RdKafkaType type, IntPtr conf,
                StringBuilder errstr,
                UIntPtr errstr_size);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern void rd_kafka_destroy(IntPtr rk);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* const char * */ IntPtr rd_kafka_name(IntPtr rk);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr rd_kafka_brokers_add(IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string brokerlist);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr rd_kafka_poll(IntPtr rk, IntPtr timeout_ms);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern SafeTopicHandle rd_kafka_topic_new(
                IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string topic,
                /* rd_kafka_topic_conf_t * */ IntPtr conf);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr rd_kafka_outq_len(IntPtr rk);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern ErrorCode rd_kafka_metadata(
            IntPtr rk, bool all_topics,
            /* rd_kafka_topic_t * */ IntPtr only_rkt,
            /* const struct rd_kafka_metadata ** */ out IntPtr metadatap,
            IntPtr timeout_ms);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern void rd_kafka_metadata_destroy(
                /* const struct rd_kafka_metadata * */ IntPtr metadata);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern ErrorCode rd_kafka_subscribe(IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr topics);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern ErrorCode rd_kafka_unsubscribe(IntPtr rk);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern /* rd_kafka_message_t * */ IntPtr rd_kafka_consumer_poll(
                IntPtr rk, IntPtr timeout_ms);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern void rd_kafka_message_destroy(/* rd_kafka_message_t * */ IntPtr rkmessage);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern ErrorCode rd_kafka_consumer_close(IntPtr rk);

        /*
        RD_EXPORT
        rd_kafka_resp_err_t rd_kafka_consumer_get_offset (rd_kafka_topic_t *rkt,
                                                          int32_t partition,
                                                          int64_t *offsetp,
                                                          IntPtr timeout_ms);
        */

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern ErrorCode rd_kafka_assign(IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr partitions);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern ErrorCode rd_kafka_assignment(IntPtr rk,
                /* rd_kafka_topic_partition_list_t ** */ out IntPtr topics);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern ErrorCode rd_kafka_subscription(IntPtr rk,
                /* rd_kafka_topic_partition_list_t ** */ out IntPtr topics);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern /* rd_kafka_topic_partition_list_t * */ IntPtr
        rd_kafka_topic_partition_list_new(IntPtr size);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern void rd_kafka_topic_partition_list_destroy(
                /* rd_kafka_topic_partition_list_t * */ IntPtr rkparlist);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern /* rd_kafka_topic_partition_t * */ IntPtr
        rd_kafka_topic_partition_list_add(
                /* rd_kafka_topic_partition_list_t * */ IntPtr rktparlist,
                string topic, int partition);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern ErrorCode rd_kafka_commit(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                bool async);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern /* char * */ IntPtr rd_kafka_memberid(IntPtr rk);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern ErrorCode rd_kafka_poll_set_consumer(IntPtr rk);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern void rd_kafka_set_log_level(IntPtr rk, int level);

        private SafeKafkaHandle() {}

        internal static SafeKafkaHandle Create(RdKafkaType type, IntPtr config)
        {
            var errorStringBuilder = new StringBuilder(512);
            var skh = rd_kafka_new(type, config, errorStringBuilder,
                    (UIntPtr) errorStringBuilder.Capacity);
            if (skh.IsInvalid)
            {
                throw new InvalidOperationException(errorStringBuilder.ToString());
            }
            if (type == RdKafkaType.Consumer)
            {
                ErrorCode err = rd_kafka_poll_set_consumer(skh.handle);
                if (err != ErrorCode.NO_ERROR)
                {
                    throw RdKafkaException.FromErr(err, "rd_kafka_poll_set_consumer failed");
                }
            }
            return skh;
        }

        protected override bool ReleaseHandle()
        {
            rd_kafka_destroy(handle);
            return true;
        }

        internal string GetName() => Marshal.PtrToStringAnsi(rd_kafka_name(handle));

        internal long GetOutQueueLength() => (long)rd_kafka_outq_len(handle);

        internal long AddBrokers(string brokers) => (long)rd_kafka_brokers_add(handle, brokers);

        internal long Poll(IntPtr timeoutMs) => (long)rd_kafka_poll(handle, timeoutMs);

        internal SafeTopicHandle Topic(string topic, IntPtr config)
        {
            // Increase the refcount to this handle to keep it alive for
            // at least as long as the topic handle.
            // Will be decremented by the topic handle ReleaseHandle.
            bool success = false;
            DangerousAddRef(ref success);
            if (!success)
            {
                SafeTopicConfigHandle.rd_kafka_topic_conf_destroy(config);
                throw new Exception("Failed to create topic (DangerousAddRef failed)");
            }
            var topicHandle = rd_kafka_topic_new(handle, topic, config);
            if (topicHandle.IsInvalid)
            {
                DangerousRelease();
                ErrorCode err = RdKafkaException.rd_kafka_errno2err(
                        (IntPtr) Marshal.GetLastWin32Error());
                throw RdKafkaException.FromErr(err, "Failed to create topic");
            }
            topicHandle.kafkaHandle = this;
            return topicHandle;
        }

        private static int[] MarshalCopy(IntPtr source, int length)
        {
            int[] res = new int[length];
            Marshal.Copy(source, res, 0, length);
            return res;
        }

        /*
         *  allTopics  - if true: request info about all topics in cluster,
         *               else: only request info about locally known topics.
         *  onlyTopic  - only request info about this topic
         *  timeout    - maximum response time before failing.
         */
        internal Metadata Metadata(bool allTopics,
                SafeTopicHandle onlyTopic,
                bool includeInternal,
                TimeSpan timeout)
        {
            if (timeout == default(TimeSpan))
            {
                timeout = TimeSpan.FromSeconds(10);
            }

            IntPtr metaPtr;
            ErrorCode err = rd_kafka_metadata(
                handle, allTopics,
                onlyTopic?.DangerousGetHandle() ?? IntPtr.Zero,
                /* const struct rd_kafka_metadata ** */ out metaPtr,
                (IntPtr) timeout.TotalMilliseconds);

            if (err == ErrorCode.NO_ERROR)
            {
                try {
                    var meta = (rd_kafka_metadata) Marshal.PtrToStructure<rd_kafka_metadata>(metaPtr);

                    var brokers = Enumerable.Range(0, meta.broker_cnt)
                        .Select(i => Marshal.PtrToStructure<rd_kafka_metadata_broker>(
                                    meta.brokers + i * Marshal.SizeOf<rd_kafka_metadata_broker>()))
                        .Select(b => new BrokerMetadata() { BrokerId = b.id, Host = b.host, Port = b.port })
                        .ToList();

                    // TODO: filter our topics starting with __, as those are internal. Maybe add a flag to not ignore them.
                    var topics = Enumerable.Range(0, meta.topic_cnt)
                        .Select(i => Marshal.PtrToStructure<rd_kafka_metadata_topic>(
                                    meta.topics + i * Marshal.SizeOf<rd_kafka_metadata_topic>()))
                        .Where(t => includeInternal || !t.topic.StartsWith("__"))
                        .Select(t => new TopicMetadata()
                                {
                                    Topic = t.topic,
                                    Error = t.err,
                                    Partitions =
                                        Enumerable.Range(0, t.partition_cnt)
                                        .Select(j => Marshal.PtrToStructure<rd_kafka_metadata_partition>(
                                                    t.partitions + j * Marshal.SizeOf<rd_kafka_metadata_partition>()))
                                        .Select(p => new PartitionMetadata()
                                                {
                                                    PartitionId = p.id,
                                                    Error = p.err,
                                                    Leader = p.leader,
                                                    Replicas = MarshalCopy(p.replicas, p.replica_cnt),
                                                    InSyncReplicas = MarshalCopy(p.isrs, p.isr_cnt)
                                                })
                                        .ToList()
                                })
                        .ToList();

                    return new Metadata()
                    {
                        Brokers = brokers,
                        Topics = topics,
                        OriginatingBrokerId = meta.orig_broker_id,
                        OriginatingBrokerName = meta.orig_broker_name
                    };
                }
                finally
                {
                    rd_kafka_metadata_destroy(metaPtr);
                }
            }
            else
            {
                throw RdKafkaException.FromErr(err, "Could not retrieve metadata");
            }
        }

        // Consumer API
        internal void Subscribe(IList<string> topics)
        {
            IntPtr list = rd_kafka_topic_partition_list_new((IntPtr) topics.Count);
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create topic partition list");
            }
            foreach (string topic in topics)
            {
                rd_kafka_topic_partition_list_add(list, topic, RD_KAFKA_PARTITION_UA);
            }

            ErrorCode err = rd_kafka_subscribe(handle, list);
            rd_kafka_topic_partition_list_destroy(list);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to subscribe to topics");
            }
        }

        internal void Unsubscribe()
        {
            ErrorCode err = rd_kafka_unsubscribe(handle);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to unsubscribe");
            }
        }

        internal MessageAndError? ConsumerPoll(IntPtr timeoutMs)
        {
            IntPtr msgPtr = rd_kafka_consumer_poll(handle, timeoutMs);
            if (msgPtr == IntPtr.Zero)
            {
                return null;
            }
            var msg = Marshal.PtrToStructure<rd_kafka_message>(msgPtr);
            byte[] payload = null;
            byte[] key = null;
            if (msg.payload != IntPtr.Zero)
            {
                payload = new byte[(int) msg.len];
                Marshal.Copy(msg.payload, payload, 0, (int) msg.len);
            }
            if (msg.key != IntPtr.Zero)
            {
                key = new byte[(int) msg.key_len];
                Marshal.Copy(msg.key, key, 0, (int) msg.key_len);
            }
            string topic = Marshal.PtrToStringAnsi(SafeTopicHandle.rd_kafka_topic_name(msg.rkt));
            rd_kafka_message_destroy(msgPtr);

            var message = new Message()
            {
                Topic = topic,
                Partition = msg.partition,
                Offset = msg.offset,
                Payload = payload,
                Key = key
            };

            return new MessageAndError()
            {
                Message = message,
                Error = msg.err
            };
        }

        internal void ConsumerClose()
        {
            ErrorCode err = rd_kafka_consumer_close(handle);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to close consumer");
            }
        }

        internal List<TopicPartition> GetAssignment()
        {
            IntPtr listPtr = IntPtr.Zero;
            ErrorCode err = rd_kafka_assignment(handle, out listPtr);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to get assignment");
            }
            // TODO: need to free anything here?
            return LibRdKafka.GetTopicPartitionList(listPtr);
        }

        internal List<string> GetSubscription()
        {
            IntPtr listPtr = IntPtr.Zero;
            ErrorCode err = rd_kafka_subscription(handle, out listPtr);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to get subscription");
            }
            // TODO: need to free anything here?
            return LibRdKafka.GetTopicList(listPtr);
        }

        internal void Assign(List<TopicPartitionOffset> partitions)
        {
            IntPtr list = rd_kafka_topic_partition_list_new((IntPtr) partitions.Count);
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create topic partition list");
            }
            foreach (var partition in partitions)
            {
                IntPtr ptr = rd_kafka_topic_partition_list_add(list, partition.Topic, partition.Partition);
                Marshal.WriteInt64(ptr,
                        (int) Marshal.OffsetOf<LibRdKafka.rd_kafka_topic_partition>("offset"),
                        partition.Offset);
            }

            ErrorCode err = rd_kafka_assign(handle, list);
            rd_kafka_topic_partition_list_destroy(list);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to assign partitions");
            }
        }

        internal void Commit()
        {
            ErrorCode err = rd_kafka_commit(handle, IntPtr.Zero, false);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to commit offsets");
            }
        }

        internal void Commit(List<TopicPartitionOffset> offsets)
        {
            IntPtr list = rd_kafka_topic_partition_list_new((IntPtr) offsets.Count);
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create offset commit list");
            }
            foreach (var offset in offsets)
            {
                IntPtr ptr = rd_kafka_topic_partition_list_add(list, offset.Topic, offset.Partition);
                Marshal.WriteInt64(ptr,
                        (int) Marshal.OffsetOf<LibRdKafka.rd_kafka_topic_partition>("offset"),
                        offset.Offset);
            }
            ErrorCode err = rd_kafka_commit(handle, list, false);
            rd_kafka_topic_partition_list_destroy(list);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to commit offsets");
            }
        }

        internal string MemberId()
        {
            IntPtr strPtr = rd_kafka_memberid(handle);
            if (strPtr == null)
            {
                return null;
            }

            string memberId = Marshal.PtrToStringAnsi(strPtr);
            LibRdKafka.rd_kafka_mem_free(handle, strPtr);
            return memberId;
        }

        internal void SetLogLevel(int level)
        {
            rd_kafka_set_log_level(handle, level);
        }
    }
}
