using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RdKafka;

namespace BenchmarkProducer
{
    public class Program
    {
        public static void Produce(string topicName, long numMessages)
        {
            using (Producer producer = new Producer("127.0.0.1:9092"))
            {
                Topic topic = producer.Topic(topicName);
                Console.WriteLine($"{producer.Name} producing on {topic.Name}");
                for (int i = 0; i < numMessages; i++)
                {
                    byte[] data = Encoding.UTF8.GetBytes(i.ToString());
                    topic.Produce(data);
                    // TODO; add continuation, count success/failures
                }

                Console.WriteLine("Shutting down");
            }

        }

        public static void Main(string[] args)
        {
            var stopwatch = new Stopwatch();

            long numMessages = 1000000;
            int numThreads = 4;

            stopwatch.Start();

            var tasks = Enumerable.Range(0, numThreads)
                .Select(x => Task.Factory.StartNew(() => Produce(args[0], numMessages),
                    CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default))
                .ToArray();
            Task.WaitAll(tasks);

            stopwatch.Stop();
            Console.WriteLine($"Sent {numMessages * numThreads} messages in {stopwatch.Elapsed}");
            Console.WriteLine($"{numMessages * numThreads / stopwatch.Elapsed.TotalSeconds:F0} messages/second");
        }
    }
}
