using System;
using System.Linq;
using System.Text;
using RdKafka;

namespace SimpleProducer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var config = new Config() { GroupId = "simple-csharp-consumer" };
            using (var consumer = new Consumer(config, "127.0.0.1:9092"))
            {
                consumer.OnMessage += (obj, msg) =>
                {
                    string text = Encoding.UTF8.GetString(msg.Payload, 0, msg.Payload.Length);
                    Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {text}");
                };

                consumer.Subscribe(args.ToList());
                consumer.Start();

                Console.WriteLine("Started consumer, press enter to stop consuming");
                Console.ReadLine();
            }
        }
    }
}
