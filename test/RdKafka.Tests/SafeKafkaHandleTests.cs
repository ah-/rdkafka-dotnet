using System;
using System.Linq;
using Xunit;
using RdKafka.Internal;

namespace RdKafka.Tests
{
    public class SafeKafkaHandleTests
    {
        [Fact]
        public void CreateHandlesErrors()
        {
            var config = new Config();
            // Trigger error
            LibRdKafka.rd_kafka_conf_set_socket_cb(config.DangerousGetHandle(), IntPtr.Zero);
            Assert.Throws<InvalidOperationException>(
                    () => KafkaSafeHandle.Create(RdKafkaType.Producer, config));
        }

        [Fact]
        public void CanGetName()
        {
            var handle = KafkaSafeHandle.Create(RdKafkaType.Producer);
            Assert.Equal(handle.Name, "rdkafka#producer-1");
        }
    }
}
