using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using RdKafka;

namespace AdvancedConsumer
{
    public class Program
    {
        public static async Task Run(string topic)
        {
            bool enableAutoCommit = false;

            var config = new Config()
            {
                GroupId = "advanced-csharp-consumer",
                EnableAutoCommit = enableAutoCommit
            };
     
            var consumer = new Consumer(config, "127.0.0.1:9092");

            consumer.OnMessage += (obj, msg) => {
                string text = Encoding.UTF8.GetString(msg.Payload, 0, msg.Payload.Length);
                Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {text}");

                if (!enableAutoCommit && msg.Offset % 10 == 0)
                {
                    Console.WriteLine($"Committing offset");
                    consumer.Commit(msg);
                    Console.WriteLine($"Committed offset");
                }
            };

            consumer.OnEndReached += (obj, end) => {
                Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");
            };

            if (enableAutoCommit)
            {
                consumer.OnOffsetCommit += (obj, commit) => {
                    if (commit.Error != ErrorCode.NO_ERROR)
                    {
                        Console.WriteLine($"Failed to commit offsets: {commit.Error}");
                    }
                    Console.WriteLine($"Successfully committed offsets: [{string.Join(", ", commit.Offsets)}]");
                };
            }

            consumer.OnPartitionsAssigned += (obj, partitions) => {
                Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]");
            };

            consumer.OnPartitionsRevoked += (obj, partitions) => {
                Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
            };

            consumer.Subscribe(topic);
            consumer.Start();

            List<TopicPartition> subscriptions = consumer.GetSubscriptions();
            Console.WriteLine($"Subscribed to: [{string.Join(", ", subscriptions)}]");

            Console.WriteLine("Started consumer, press enter to stop consuming");
            Console.ReadLine();
            await consumer.Stop();
        }

        public static void Main(string[] args)
        {
            Run(args[0]).Wait();
        }
    }
}
