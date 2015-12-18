using System;
using System.Linq;
using Xunit;
using RdKafka.Internal;

namespace RdKafka.Tests
{
    public class SafeKafkaHandleTests
    {
        [Fact]
        public void CanGetName()
        {
            var producer = new Producer("localhost:9092");
            Assert.Equal(producer.Name, "rdkafka#producer-1");
        }
    }
}
