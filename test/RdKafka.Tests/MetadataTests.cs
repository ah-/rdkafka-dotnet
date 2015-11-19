using System;
using System.Linq;
using Xunit;
using RdKafka;

// TODO: roll this into some other tests, can't test much more here
namespace RdKafka.Tests
{
    public class MetadataTests
    {
        [Fact]
        public void FetchingMetadataWorks()
        {
            var client = KafkaSafeHandle.Create(RdKafkaType.Producer, Config.Create());
            client.AddBrokers("localhost:9092");

            var meta = client.Metadata();
            Assert.True(meta.Brokers.Count >= 1);
            Assert.Equal(meta.Brokers.First().Port, 9092);
            Assert.True(meta.Topics.Count >= 1);
        }
    }
}
