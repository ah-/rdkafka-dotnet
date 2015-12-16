using System;
using System.Runtime.InteropServices;

namespace RdKafka.Internal
{
    [StructLayout(LayoutKind.Sequential)]
    struct rd_kafka_metadata_broker {
        internal int id;
        internal string host;
        internal int port;
    }

    [StructLayout(LayoutKind.Sequential)]
    struct rd_kafka_metadata_partition {
        internal int id;
        internal ErrorCode err;
        internal int leader;
        internal int replica_cnt;
        internal /* int32_t * */ IntPtr replicas;
        internal int isr_cnt;
        internal /* int32_t * */ IntPtr isrs;
    }

    [StructLayout(LayoutKind.Sequential)]
    struct rd_kafka_metadata_topic {
        internal string topic;
        internal int partition_cnt;
        internal /* struct rd_kafka_metadata_partition * */ IntPtr partitions;
        internal ErrorCode err;
    }

    [StructLayout(LayoutKind.Sequential)]
    struct rd_kafka_metadata {
        internal int broker_cnt;
        internal /* struct rd_kafka_metadata_broker * */ IntPtr brokers;
        internal int topic_cnt;
        internal /* struct rd_kafka_metadata_topic * */ IntPtr topics;
        internal int orig_broker_id;
        [MarshalAs(UnmanagedType.LPStr)]
        internal string orig_broker_name;
    };
}
