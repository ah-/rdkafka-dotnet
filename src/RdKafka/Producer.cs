using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using RdKafka.Internal;

namespace RdKafka
{
    /// <summary>
    /// High-level, asynchronous message producer.
    /// </summary>
    public class Producer : Handle
    {
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
            return new Topic(handle, this, topic, config);
        }

        // Explicitly keep reference to delegate so it stays alive
        static LibRdKafka.DeliveryReportCallback DeliveryReportDelegate = DeliveryReportCallback;

        static void DeliveryReportCallback(IntPtr rk,
                ref rd_kafka_message rkmessage, IntPtr opaque)
        {
            // msg_opaque was set by Topic.Produce
            var gch = GCHandle.FromIntPtr(rkmessage._private);
            var deliveryCompletionSource = (TaskCompletionSource<DeliveryReport>) gch.Target;
            gch.Free();

            if (rkmessage.err != 0)
            {
                deliveryCompletionSource.SetException(
                    RdKafkaException.FromErr(
                        rkmessage.err,
                        Marshal.PtrToStringAnsi(rkmessage.payload)));
                return;
            }

            deliveryCompletionSource.SetResult(new DeliveryReport() {
                Offset = rkmessage.offset,
                Partition = rkmessage.partition
            });
        }
    }
}
