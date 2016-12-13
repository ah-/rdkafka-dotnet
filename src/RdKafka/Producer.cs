using System;
using System.Runtime.InteropServices;
using RdKafka.Internal;
using System.Collections.Concurrent;

namespace RdKafka
{
    /// <summary>
    /// High-level, asynchronous message producer.
    /// </summary>
    public class Producer : Handle
    {
        BlockingCollection<LibRdKafka.PartitionerCallback> topicPartitioners 
            = new BlockingCollection<LibRdKafka.PartitionerCallback>();

        public Producer(string brokerList) : this(null, brokerList) {}

        public Producer(Config config, string brokerList = null)
        {
            config = config ?? new Config();

            IntPtr cfgPtr = config.handle.Dup();
            LibRdKafka.conf_set_dr_msg_cb(cfgPtr, DeliveryReportDelegate);
            Init(RdKafkaType.Producer, cfgPtr, config.Logger);

            if (brokerList != null)
            {
                handle.AddBrokers(brokerList);
            }
        }

        public Topic Topic(string topic, TopicConfig config = null)
        {
            LibRdKafka.PartitionerCallback partitionerDelegate;
            var kafkaTopic = new Topic(handle, this, topic, config, out partitionerDelegate);
            if (config?.CustomPartitioner != null)
            {
                // kafkaTopic may be GC before partitionerDelegate is called on all produced mesages
                // so we need to keep a reference to it.
                // We can't make it static in Topic as the partitioner will be different for each topic
                // we could make it in a static collection in Topic, but we can clear it when producer is closed,
                // (as it wait for all message to be produced)
                // so putting it in an instance collection allows us to free it eventually

                // it's not very effective for people creating a lot of topics
                // we should find a way to clear the list 
                // when there is no more messages in queue related to the topic
                topicPartitioners.Add(partitionerDelegate);
            }
            return kafkaTopic;
        }

        // Explicitly keep reference to delegate so it stays alive
        private static readonly LibRdKafka.DeliveryReportCallback DeliveryReportDelegate = DeliveryReportCallback;

        private static void DeliveryReportCallback(IntPtr rk, ref rd_kafka_message rkmessage, IntPtr opaque)
        {
            // msg_opaque was set by Topic.Produce
            var gch = GCHandle.FromIntPtr(rkmessage._private);
            var deliveryHandler = (IDeliveryHandler) gch.Target;
            gch.Free();

            if (rkmessage.err != 0)
            {
                deliveryHandler.SetException(
                    RdKafkaException.FromErr(
                        rkmessage.err,
                        "Failed to produce message"));
                return;
            }

            deliveryHandler.SetResult(new DeliveryReport {
                Offset = rkmessage.offset,
                Partition = rkmessage.partition
            });
        }
    }
}
