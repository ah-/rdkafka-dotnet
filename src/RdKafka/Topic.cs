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

    public class Topic : IDisposable
    {
        const int RD_KAFKA_PARTITION_UA = -1;

        internal readonly SafeTopicHandle handle;
        readonly Producer producer;
        readonly LibRdKafka.PartitionerCallback PartitionerDelegate;

        internal Topic(SafeKafkaHandle kafkaHandle, Producer producer, string topic, TopicConfig config)
        {
            this.producer = producer;

            config = config ?? new TopicConfig();
            config["produce.offset.report"] = "true";
            IntPtr configPtr = config.handle.Dup();

            if (config.CustomPartitioner != null)
            {
                PartitionerDelegate = (IntPtr rkt, IntPtr keydata, UIntPtr keylen, int partition_cnt,
                        IntPtr rkt_opaque, IntPtr msg_opaque) =>
                {
                    byte[] key = null;
                    if (keydata != IntPtr.Zero)
                    {
                        key = new byte[(int) keylen];
                        Marshal.Copy(keydata, key, 0, (int) keylen);
                    }
                    return config.CustomPartitioner(this, key, partition_cnt);
                };
                LibRdKafka.topic_conf_set_partitioner_cb(configPtr, PartitionerDelegate);
            }

            handle = kafkaHandle.Topic(topic, configPtr);
        }

        public void Dispose()
        {
            handle.Dispose();
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

                ErrorCode err = LibRdKafka.errno2err(
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

        /// <summary>
        /// Check if partition is available (has a leader broker).
        ///
        /// Return true if the partition is available, else false.
        ///
        /// This function must only be called from inside a partitioner function.
        /// </summary>
        public bool PartitionAvailable(int partition)
        {
            return handle.PartitionAvailable(partition);
        }
    }
}
