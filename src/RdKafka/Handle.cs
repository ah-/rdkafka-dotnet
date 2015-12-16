using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using RdKafka.Internal;

namespace RdKafka
{
    public class Handle
    {
        internal SafeKafkaHandle handle;

        public string Name => handle.GetName();

        // public string MemberId => handle.MemberId()

        public long OutQueueLength => handle.GetOutQueueLength();

        /// <summary>
        /// Request Metadata from broker.
        ///
        /// Parameters:
        ///   allTopics    - if true: request info about all topics in cluster,
        ///                  if false: only request info about locally known topics.
        ///   onlyForTopic - only request info about this topic
        ///   includeInternal - include internal topics prefixed with __
        ///   timeout      - maximum response time before failing.
        /// </summary>
        public Metadata Metadata (bool allTopics=true, Topic onlyForTopic=null,
                bool includeInternal=false, TimeSpan timeout=default(TimeSpan))
        {
            return handle.Metadata(allTopics, onlyForTopic?.handle, includeInternal, timeout);
        }
    }
}
