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

        /// <summary>
        /// The name of the handle
        /// </summary>
        public string Name => handle.GetName();

        /// <summary>
        /// The client's broker-assigned group member id
        ///
        /// Last assigned member id, or empty string if not currently
        /// a group member.
        /// </summary>
        public string MemberId => handle.MemberId();

        /// <summary>
        /// The current out queue length
        ///
        /// The out queue contains messages and requests waiting to be sent to,
        /// or acknowledged by, the broker.
        /// </summary>
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
