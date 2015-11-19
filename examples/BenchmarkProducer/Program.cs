using System;
using System.Diagnostics;
using System.Text;
using RdKafka;

namespace BenchmarkProducer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var stopwatch = new Stopwatch();

            long numMessages = 1000000;

            using (Producer producer = new Producer("127.0.0.1:9092"))
            {
                Topic topic = producer.Topic(args[0]);
                Console.WriteLine($"{producer.Name} producing on {topic.Name}");

                stopwatch.Start();

                for (int i = 0; i < numMessages; i++)
                {
                    byte[] data = Encoding.UTF8.GetBytes(i.ToString());
                    topic.Produce(data);
                    // TODO; add continuation, count success/failures
                }

                Console.WriteLine("Shutting down");
            }

            stopwatch.Stop();
            Console.WriteLine($"Sent {numMessages} messages in {stopwatch.Elapsed}");
            Console.WriteLine($"{numMessages / stopwatch.Elapsed.TotalSeconds:F0} messages/second");
        }
    }
}
