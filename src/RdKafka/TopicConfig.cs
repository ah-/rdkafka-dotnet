using System.Collections.Generic;
using RdKafka.Internal;

namespace RdKafka
{
    public class TopicConfig
    {
        internal readonly SafeTopicConfigHandle handle;

        public TopicConfig()
        {
            handle = SafeTopicConfigHandle.Create();
        }

        public Dictionary<string, string> Dump() => handle.Dump();

        public string this[string name]
        {
            set
            {
                handle.Set(name, value);
            }
            get
            {
                return handle.Get(name);
            }
        }

        public delegate int Partitioner(Topic topic, byte[] key, int partitionCount);
        public Partitioner CustomPartitioner { get; set; }
    }
}
