using System;
using System.Runtime.InteropServices;

namespace RdKafka.Internal
{
    enum MsgFlags
    {
        MSG_F_FREE = 1,
        MSG_F_COPY = 2
    }
 
    internal sealed class SafeTopicHandle : SafeHandleZeroIsInvalid
    {
        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_topic_destroy(IntPtr rk);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* const char * */ IntPtr rd_kafka_topic_name(IntPtr rkt);

        [DllImport("librdkafka", SetLastError = true, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_produce(
                IntPtr rkt,
                int partition,
                IntPtr msgflags,
                byte[] payload, UIntPtr len,
                byte[] key, UIntPtr keylen,
                IntPtr msg_opaque);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern bool rd_kafka_topic_partition_available(IntPtr rkt, int partition);

        const int RD_KAFKA_PARTITION_UA = -1;

        internal SafeKafkaHandle kafkaHandle;

        private SafeTopicHandle() { }

        protected override bool ReleaseHandle()
        {
            rd_kafka_topic_destroy(handle);
            // See SafeKafkaHandle.Topic
            kafkaHandle.DangerousRelease();
            return true;
        }

        internal string GetName() => Marshal.PtrToStringAnsi(rd_kafka_topic_name(handle));

        internal long Produce(byte[] payload, byte[] key, int partition, IntPtr opaque)
        {
            return (long) rd_kafka_produce(
                    handle,
                    partition,
                    (IntPtr) MsgFlags.MSG_F_COPY,
                    payload, (UIntPtr) (payload?.Length ?? 0),
                    key, (UIntPtr) (key?.Length ?? 0),
                    opaque);
        }

        internal bool PartitionAvailable(int partition)
        {
            return rd_kafka_topic_partition_available(handle, partition);
        }
    }
}
