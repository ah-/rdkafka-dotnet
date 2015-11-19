using System.Collections.Generic;

namespace RdKafka
{
    public struct Metadata
    {
        public List<BrokerMetadata> Brokers { get; set; }
        public List<TopicMetadata> Topics { get; set; }
        public int OriginatingBrokerId { get; set; }
        public string OriginatingBrokerName { get; set; }
    }

    public struct BrokerMetadata
    {
        public int BrokerId { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
    }

    public struct PartitionMetadata
    {
        public int PartitionId { get; set; }
        public int Leader { get; set; }
        public int[] Replicas { get; set; }
        public int[] InSyncReplicas { get; set; }
        public ErrorCode Error { get; set; }
    }

    public struct TopicMetadata
    {
        public string Topic { get; set; }
        public List<PartitionMetadata> Partitions { get; set; }
        public ErrorCode Error { get; set; }
    }

    public struct TopicPartition
    {
        public string Topic { get; set; }
        public int Partition { get; set; }

        public override string ToString() => Topic + " " + Partition;
    }

    public struct TopicPartitionOffset
    {
        public string Topic { get; set; }
        public int Partition { get; set; }
        public long Offset { get; set; }

        public override string ToString() => Topic + " " + Partition + " " + Offset;
    }
}
