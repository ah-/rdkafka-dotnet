using System;
using System.Text;
using System.Threading.Tasks;
using RdKafka;

namespace SimpleProducer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Topic topic;
            using (Producer producer = new Producer("127.0.0.1:9092"))
            {
                topic = producer.Topic(args[0]);
                Console.WriteLine($"{producer.Name} producing on {topic.Name}. q to exit.");

                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    byte[] data = Encoding.UTF8.GetBytes(text);
                    Task<DeliveryReport> deliveryReport = topic.Produce(data);
                    var unused = deliveryReport.ContinueWith(task =>
                    {
                        Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                    });
                }
            }
        }
    }
}
