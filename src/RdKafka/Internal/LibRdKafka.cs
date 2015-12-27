using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace RdKafka.Internal
{
    internal static class LibRdKafka
    {
        static LibRdKafka()
        {
            // TODO: check if win desktop .net, then preload from x86/x64
            // TODO: check rd_kafka_version first, throw if too low

            // Warning: madness below, due to mono needing to load __Internal
            if (PlatformApis.IsDarwinMono)
            {
                _version = NativeDarwinMonoMethods.rd_kafka_version;
                _version_str = NativeDarwinMonoMethods.rd_kafka_version_str;
                _get_debug_contexts = NativeDarwinMonoMethods.rd_kafka_get_debug_contexts;
                _err2str = NativeDarwinMonoMethods.rd_kafka_err2str;
                _errno2err = NativeDarwinMonoMethods.rd_kafka_errno2err;
                _set_log_level = NativeDarwinMonoMethods.rd_kafka_set_log_level;
                _wait_destroyed = NativeDarwinMonoMethods.rd_kafka_wait_destroyed;
                _conf_set_default_topic_conf = NativeDarwinMonoMethods.rd_kafka_conf_set_default_topic_conf;
                _conf_set_dr_msg_cb = NativeDarwinMonoMethods.rd_kafka_conf_set_dr_msg_cb;
                _conf_set_rebalance_cb = NativeDarwinMonoMethods.rd_kafka_conf_set_rebalance_cb;
                _conf_set_offset_commit_cb = NativeDarwinMonoMethods.rd_kafka_conf_set_offset_commit_cb;
                _conf_set_log_cb = NativeDarwinMonoMethods.rd_kafka_conf_set_log_cb;
                _conf_set_stats_cb = NativeDarwinMonoMethods.rd_kafka_conf_set_stats_cb;
                _topic_conf_set_partitioner_cb = NativeDarwinMonoMethods.rd_kafka_topic_conf_set_partitioner_cb;
                _consumer_poll = NativeDarwinMonoMethods.rd_kafka_consumer_poll;
                _mem_free = NativeDarwinMonoMethods.rd_kafka_mem_free;
            }
            else
            {
                _version = NativeMethods.rd_kafka_version;
                _version_str = NativeMethods.rd_kafka_version_str;
                _get_debug_contexts = NativeMethods.rd_kafka_get_debug_contexts;
                _err2str = NativeMethods.rd_kafka_err2str;
                _errno2err = NativeMethods.rd_kafka_errno2err;
                _set_log_level = NativeMethods.rd_kafka_set_log_level;
                _wait_destroyed = NativeMethods.rd_kafka_wait_destroyed;
                _conf_set_default_topic_conf = NativeMethods.rd_kafka_conf_set_default_topic_conf;
                _conf_set_dr_msg_cb = NativeMethods.rd_kafka_conf_set_dr_msg_cb;
                _conf_set_rebalance_cb = NativeMethods.rd_kafka_conf_set_rebalance_cb;
                _conf_set_offset_commit_cb = NativeMethods.rd_kafka_conf_set_offset_commit_cb;
                _conf_set_log_cb = NativeMethods.rd_kafka_conf_set_log_cb;
                _conf_set_stats_cb = NativeMethods.rd_kafka_conf_set_stats_cb;
                _topic_conf_set_partitioner_cb = NativeMethods.rd_kafka_topic_conf_set_partitioner_cb;
                _consumer_poll = NativeMethods.rd_kafka_consumer_poll;
                _mem_free = NativeMethods.rd_kafka_mem_free;
            }
        }

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


        private static Func<IntPtr> _version;
        internal static IntPtr version() => _version();

        private static Func<IntPtr> _version_str;
        internal static IntPtr version_str() => _version_str();

        private static Func<IntPtr> _get_debug_contexts;
        internal static IntPtr get_debug_contexts() => _get_debug_contexts();

        private static Func<ErrorCode, IntPtr> _err2str;
        internal static IntPtr err2str(ErrorCode err) => _err2str(err);

        private static Func<IntPtr, ErrorCode> _errno2err;
        internal static ErrorCode errno2err(IntPtr errno) => errno2err(errno);

        private static Action<IntPtr, IntPtr> _set_log_level;
        internal static void set_log_level(IntPtr rk, IntPtr level)
            => _set_log_level(rk, level);

        private static Func<IntPtr, IntPtr> _wait_destroyed;
        internal static IntPtr wait_destroyed(IntPtr timeout_ms)
            => _wait_destroyed(timeout_ms);

        private static Action<IntPtr, IntPtr> _conf_set_default_topic_conf;
        internal static void conf_set_default_topic_conf(IntPtr conf, IntPtr tconf)
            => _conf_set_default_topic_conf(conf, tconf);

        private static Action<IntPtr, DeliveryReportCallback> _conf_set_dr_msg_cb;
        internal static void conf_set_dr_msg_cb(IntPtr conf, DeliveryReportCallback dr_msg_cb)
            => _conf_set_dr_msg_cb(conf, dr_msg_cb);

        private static Action<IntPtr, RebalanceCallback> _conf_set_rebalance_cb;
        internal static void conf_set_rebalance_cb(IntPtr conf, RebalanceCallback rebalance_cb)
            => _conf_set_rebalance_cb(conf, rebalance_cb);

        private static Action<IntPtr, CommitCallback> _conf_set_offset_commit_cb;
        internal static void conf_set_offset_commit_cb(IntPtr conf, CommitCallback commit_cb)
            => _conf_set_offset_commit_cb(conf, commit_cb);

        private static Action<IntPtr, LogCallback> _conf_set_log_cb;
        internal static void conf_set_log_cb(IntPtr conf, LogCallback log_cb)
            => _conf_set_log_cb(conf, log_cb);

        private static Action<IntPtr, StatsCallback> _conf_set_stats_cb;
        internal static void conf_set_stats_cb(IntPtr conf, StatsCallback stats_cb)
            => _conf_set_stats_cb(conf, stats_cb);

        private static Action<IntPtr, PartitionerCallback> _topic_conf_set_partitioner_cb;
        internal static void topic_conf_set_partitioner_cb(IntPtr topic_conf, PartitionerCallback partitioner_cb)
            => _topic_conf_set_partitioner_cb(topic_conf, partitioner_cb);

        private static Func<IntPtr, IntPtr, IntPtr> _consumer_poll;
        internal static IntPtr consumer_poll(IntPtr rk, IntPtr timeout_ms)
            => _consumer_poll(rk, timeout_ms);

        private static Action<IntPtr, IntPtr> _mem_free;
        internal static void mem_free(IntPtr rk, IntPtr ptr)
            => _mem_free(rk, ptr);

        private class NativeMethods
        {
            private const string DllName = "librdkafka";

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern IntPtr rd_kafka_version();

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern IntPtr rd_kafka_version_str();

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern IntPtr rd_kafka_get_debug_contexts();

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern IntPtr rd_kafka_err2str(ErrorCode err);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern ErrorCode rd_kafka_errno2err(IntPtr errno);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_set_log_level(IntPtr rk, IntPtr level);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern IntPtr rd_kafka_wait_destroyed(IntPtr timeout_ms);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_default_topic_conf(
                    IntPtr conf, IntPtr tconf);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_dr_msg_cb(
                    IntPtr conf,
                    DeliveryReportCallback dr_msg_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_rebalance_cb(
                    IntPtr conf, RebalanceCallback rebalance_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_offset_commit_cb(
                    IntPtr conf, CommitCallback commit_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_log_cb(IntPtr conf, LogCallback log_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_stats_cb(IntPtr conf, StatsCallback stats_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_topic_conf_set_partitioner_cb(
                    IntPtr topic_conf, PartitionerCallback partitioner_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern /* rd_kafka_message_t * */ IntPtr
            rd_kafka_consumer_poll(IntPtr rk, IntPtr timeout_ms);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_mem_free(IntPtr rk, IntPtr ptr);
        }

        private class NativeDarwinMonoMethods
        {
            private const string DllName = "__Internal";

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern IntPtr rd_kafka_version();

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern IntPtr rd_kafka_version_str();

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern IntPtr rd_kafka_get_debug_contexts();

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern IntPtr rd_kafka_err2str(ErrorCode err);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern ErrorCode rd_kafka_errno2err(IntPtr errno);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_set_log_level(IntPtr rk, IntPtr level);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern IntPtr rd_kafka_wait_destroyed(IntPtr timeout_ms);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_default_topic_conf(
                    IntPtr conf, IntPtr tconf);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_dr_msg_cb(
                    IntPtr conf,
                    DeliveryReportCallback dr_msg_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_rebalance_cb(
                    IntPtr conf, RebalanceCallback rebalance_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_offset_commit_cb(
                    IntPtr conf, CommitCallback commit_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_log_cb(IntPtr conf, LogCallback log_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_stats_cb(IntPtr conf, StatsCallback stats_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_topic_conf_set_partitioner_cb(
                    IntPtr topic_conf, PartitionerCallback partitioner_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern /* rd_kafka_message_t * */ IntPtr
            rd_kafka_consumer_poll(IntPtr rk, IntPtr timeout_ms);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_mem_free(IntPtr rk, IntPtr ptr);
        }
    }
}
