using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace RdKafka.Internal
{
    static class LibRdKafka
    {
        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate void DeliveryReportCallback(
                IntPtr rk,
                /* const rd_kafka_message_t * */ ref rd_kafka_message rkmessage,
                IntPtr opaque);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate void CommitCallback(IntPtr rk,
                ErrorCode err,
                /* rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                IntPtr opaque);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate void RebalanceCallback(IntPtr rk,
                ErrorCode err,
                /* rd_kafka_topic_partition_list_t * */ IntPtr partitions,
                IntPtr opaque);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate void LogCallback(IntPtr rk, int level, string fac, string buf);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate int StatsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate int PartitionerCallback(
            /* const rd_kafka_topic_t * */ IntPtr rkt,
            IntPtr keydata,
            UIntPtr keylen,
            int partition_cnt,
            IntPtr rkt_opaque,
            IntPtr msg_opaque);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_default_topic_conf(
                IntPtr conf, IntPtr tconf);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern void rd_kafka_conf_set_dr_cb(
                IntPtr conf,
                DeliveryReportCallback dr_cb);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_dr_msg_cb(
                IntPtr conf,
                DeliveryReportCallback dr_msg_cb);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_rebalance_cb(
                IntPtr conf, RebalanceCallback rebalance_cb);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_offset_commit_cb(
                IntPtr conf, CommitCallback commit_cb);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_log_cb(IntPtr conf, LogCallback log_cb);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_stats_cb(IntPtr conf, StatsCallback stats_cb);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_topic_conf_set_partitioner_cb(
                IntPtr topic_conf, PartitionerCallback partitioner_cb);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_message_t * */ IntPtr
        rd_kafka_consumer_poll(IntPtr rk, IntPtr timeout_ms);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_mem_free(IntPtr rk, IntPtr ptr);
    }
}
