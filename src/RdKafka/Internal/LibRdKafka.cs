using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace RdKafka.Internal
{
    [StructLayout(LayoutKind.Sequential)]
    struct rd_kafka_message
    {
        internal ErrorCode err; /* Non-zero for error signaling. */
        internal /* rd_kafka_topic_t * */ IntPtr rkt; /* Topic */
        internal int partition;                 /* Partition */
        internal /* void   * */ IntPtr payload; /* err==0: Message payload
                                        * err!=0: Error string */
        internal UIntPtr  len;                  /* err==0: Message payload length
                                        * err!=0: Error string length */
        internal /* void   * */ IntPtr key;     /* err==0: Optional message key */
        internal UIntPtr  key_len;              /* err==0: Optional message key length */
        internal long offset;                  /* Consume:
                                        *   Message offset (or offset for error
                                        *   if err!=0 if applicable).
                                        * dr_msg_cb:
                                        *   Message offset assigned by broker.
                                        *   If produce.offset.report is set then
                                        *   each message will have this field set,
                                        *   otherwise only the last message in
                                        *   each produced internal batch will
                                        *   have this field set, otherwise 0. */
        internal /* void  * */ IntPtr _private; /* Consume:
                                        *   rdkafka private pointer: DO NOT MODIFY
                                        * dr_msg_cb:
                                        *   mgs_opaque from produce() call */
    }

    internal static class LibRdKafka
    {
        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern void rd_kafka_conf_set_dr_cb(
                IntPtr conf,
                DeliveryReportCallback dr_cb);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_dr_msg_cb(
                IntPtr conf,
                DeliveryReportCallback dr_msg_cb);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate void DeliveryReportCallback(
                IntPtr rk,
                /* const rd_kafka_message_t * */ ref rd_kafka_message rkmessage,
                IntPtr opaque);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_rebalance_cb(
                IntPtr conf, RebalanceCallback rebalance_cb);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate void RebalanceCallback(IntPtr rk,
                ErrorCode err,
                /* rd_kafka_topic_partition_list_t * */ IntPtr partitions,
                IntPtr opaque);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_offset_commit_cb(
                IntPtr conf, CommitCallback commit_cb);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate void CommitCallback(IntPtr rk,
                ErrorCode err,
                /* rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                IntPtr opaque);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_message_t * */ IntPtr rd_kafka_consumer_poll(
                /* rd_kafka_t * */ IntPtr rk, IntPtr timeout_ms);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_mem_free(IntPtr rk, IntPtr ptr);

        [StructLayout(LayoutKind.Sequential)]
        internal struct rd_kafka_topic_partition {
                internal string topic;
                internal int partition;
                internal long offset;
                /* void * */ IntPtr metadata;
                UIntPtr metadata_size;
                /* void * */ IntPtr opaque;
                ErrorCode err; /* Error code, depending on use. */
                /* void * */ IntPtr _private; /* INTERNAL USE ONLY,
                                               * INITIALIZE TO ZERO, DO NOT TOUCH */
        };

        [StructLayout(LayoutKind.Sequential)]
        struct rd_kafka_topic_partition_list {
                internal int cnt; /* Current number of elements */
                internal int size; /* Allocated size */
                internal /* rd_kafka_topic_partition_t * */ IntPtr elems;
        };

        internal static List<string> GetTopicList(IntPtr listPtr)
        {
            if (listPtr == IntPtr.Zero)
            {
                return new List<string>();
            }

            var list = Marshal.PtrToStructure<rd_kafka_topic_partition_list>(listPtr);
            return Enumerable.Range(0, list.cnt)
                .Select(i => Marshal.PtrToStructure<rd_kafka_topic_partition>(
                    list.elems + i * Marshal.SizeOf<rd_kafka_topic_partition>()))
                .Select(ktp => ktp.topic)
                .ToList();
        }

        internal static List<TopicPartition> GetTopicPartitionList(IntPtr listPtr)
        {
            if (listPtr == IntPtr.Zero)
            {
                return new List<TopicPartition>();
            }

            var list = Marshal.PtrToStructure<rd_kafka_topic_partition_list>(listPtr);
            return Enumerable.Range(0, list.cnt)
                .Select(i => Marshal.PtrToStructure<rd_kafka_topic_partition>(
                    list.elems + i * Marshal.SizeOf<rd_kafka_topic_partition>()))
                .Select(ktp => new TopicPartition()
                        {
                            Topic = ktp.topic,
                            Partition = ktp.partition,
                        })
                .ToList();
        }

        internal static List<TopicPartitionOffset> GetTopicPartitionOffsetList(IntPtr listPtr)
        {
            if (listPtr == IntPtr.Zero)
            {
                return new List<TopicPartitionOffset>();
            }

            var list = Marshal.PtrToStructure<rd_kafka_topic_partition_list>(listPtr);
            return Enumerable.Range(0, list.cnt)
                .Select(i => Marshal.PtrToStructure<rd_kafka_topic_partition>(
                    list.elems + i * Marshal.SizeOf<rd_kafka_topic_partition>()))
                .Select(ktp => new TopicPartitionOffset()
                        {
                            Topic = ktp.topic,
                            Partition = ktp.partition,
                            Offset = ktp.offset
                        })
                .ToList();
        }
    }
}
