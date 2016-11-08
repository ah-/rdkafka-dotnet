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
        const int RD_KAFKA_PARTITION_UA = -1;

        internal SafeKafkaHandle kafkaHandle;

        private SafeTopicHandle() { }

        protected override bool ReleaseHandle()
        {
            LibRdKafka.topic_destroy(handle);
            // See SafeKafkaHandle.Topic
            kafkaHandle.DangerousRelease();
            return true;
        }

        internal string GetName() => Marshal.PtrToStringAnsi(LibRdKafka.topic_name(handle));

        internal long Produce(ArraySegment<byte>? payload, ArraySegment<byte>? key, int partition, IntPtr opaque)
        {
            var pPayload = IntPtr.Zero;
            var pKey = IntPtr.Zero;

            var gchPayload= default(GCHandle);
            var gchKey = default(GCHandle);

            var payloadCount = 0;
            var keyCount = 0;

            if (payload.HasValue)
            {
                gchPayload = GCHandle.Alloc(payload.Value.Array, GCHandleType.Pinned);
                pPayload = GCHandle.ToIntPtr(gchPayload) + payload.Value.Offset;
                payloadCount = payload.Value.Count;
            }

            if (key.HasValue)
            {
                gchKey = GCHandle.Alloc(key.Value.Array, GCHandleType.Pinned);
                pKey = GCHandle.ToIntPtr(gchKey) + key.Value.Offset;
                keyCount = key.Value.Count;
            }
            
            try
            {
                return (long) LibRdKafka.produce(
                    handle,
                    partition,
                    (IntPtr) MsgFlags.MSG_F_COPY,
                    pPayload, (UIntPtr) payloadCount,
                    pKey, (UIntPtr) keyCount,
                    opaque);
            }
            finally
            {
                if (payload.HasValue)
                    gchPayload.Free();

                if (key.HasValue)
                    gchKey.Free();
            }
        }
        
        internal bool PartitionAvailable(int partition) => LibRdKafka.topic_partition_available(handle, partition);
    }
}
