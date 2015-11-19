using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using RdKafka.Internal;

namespace RdKafka
{
    public struct DeliveryReport
    {
        public int Partition;
        public long Offset;
    }

    public class Topic
    {
        const int RD_KAFKA_PARTITION_UA = -1;
        SafeTopicHandle handle;
        Producer producer;

        internal Topic(SafeTopicHandle handle, Producer producer)
        {
            this.handle = handle;
            this.producer = producer;
        }

        public string Name => handle.GetName();

        public Task<DeliveryReport> Produce(byte[] payload, byte[] key = null, Int32 partition = RD_KAFKA_PARTITION_UA)
        {
            // Passes the TaskCompletionSource to the delivery report callback
            // via the msg_opaque pointer
            var deliveryCompletionSource = new TaskCompletionSource<DeliveryReport>();
            var gch = GCHandle.Alloc(deliveryCompletionSource);

            while (true)
            {
                if (handle.Produce(payload, key, partition, GCHandle.ToIntPtr(gch)) == 0)
                {
                    // Successfully enqueued produce request
                    break;
                }

                ErrorCode err = RdKafkaException.rd_kafka_errno2err(
                        (IntPtr) Marshal.GetLastWin32Error());
                if (err == ErrorCode._QUEUE_FULL)
                {
                    // Wait and retry
                    Task.Delay(TimeSpan.FromMilliseconds(50)).Wait();
                }
                else
                {
                    gch.Free();
                    throw RdKafkaException.FromErr(err, "Could not produce message");
                }
            }

            return deliveryCompletionSource.Task;
        }
    }
}
