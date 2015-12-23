using System;
using RdKafka;

namespace Misc
{
    public class Program
    {
        static string ToString(int[] array) => $"[{string.Join(", ", array)}]";

        public static void Main(string[] args)
        {
            Console.WriteLine($"Hello RdKafka!");
            Console.WriteLine($"{Library.Version:X}");
            Console.WriteLine($"{Library.VersionString}");
            Console.WriteLine($"{string.Join(", ", Library.DebugContexts)}");

            using (var producer = new Producer("localhost:9092"))
            {
                var meta = producer.Metadata();
                Console.WriteLine($"{meta.OriginatingBrokerId} {meta.OriginatingBrokerName}");
                meta.Brokers.ForEach(broker =>
                    Console.WriteLine($"Broker: {broker.BrokerId} {broker.Host}:{broker.Port}"));

                meta.Topics.ForEach(topic =>
                {
                    Console.WriteLine($"Topic: {topic.Topic} {topic.Error}");
                    topic.Partitions.ForEach(partition =>
                    {
                        Console.WriteLine($"  Partition: {partition.PartitionId}");
                        Console.WriteLine($"    Replicas: {ToString(partition.Replicas)}");
                        Console.WriteLine($"    InSyncReplicas: {ToString(partition.InSyncReplicas)}");
                    });
                });
            }

            foreach (var kv in new Config().Dump())
            {
                Console.WriteLine($"\"{kv.Key}\": \"{kv.Value}\"");
            }
        }
    }
}
