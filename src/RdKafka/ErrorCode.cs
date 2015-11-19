namespace RdKafka
{
    public enum ErrorCode {
        /* Internal errors to rdkafka, prefixed with _: */
        /** Begin internal error codes */
        _BEGIN = -200,
        /** Received message is incorrect */
        _BAD_MSG = -199,
        /** Bad/unknown compression */
        _BAD_COMPRESSION = -198,
        /** Broker is going away */
        _DESTROY = -197,
        /** Generic failure */
        _FAIL = -196,
        /** Broker transport failure */
        _TRANSPORT = -195,
        /** Critical system resource */
        _CRIT_SYS_RESOURCE = -194,
        /** Failed to resolve broker */
        _RESOLVE = -193,
        /** Produced message timed out*/
        _MSG_TIMED_OUT = -192,
        /** Reached the end of the topic+partition queue on
         * the broker. Not really an error. */
        _PARTITION_EOF = -191,
        /** Permanent: Partition does not exist in cluster. */
        _UNKNOWN_PARTITION = -190,
        /** File or filesystem error */
        _FS = -189,
         /** Permanent: Topic does not exist in cluster. */
        _UNKNOWN_TOPIC = -188,
        /** All broker connections are down. */
        _ALL_BROKERS_DOWN = -187,
        /** Invalid argument, or invalid configuration */
        _INVALID_ARG = -186,
        /** Operation timed out */
        _TIMED_OUT = -185,
        /** Queue is full */
        _QUEUE_FULL = -184,
        /** ISR count < required.acks */
            _ISR_INSUFF = -183,
        /** Broker node update */
            _NODE_UPDATE = -182,
        /** SSL error */
        _SSL = -181,
        /** Waiting for coordinator to become available. */
            _WAIT_COORD = -180,
        /** Unknown client group */
            _UNKNOWN_GROUP = -179,
        /** Operation in progress */
            _IN_PROGRESS = -178,
         /** Previous operation in progress, wait for it to finish. */
            _PREV_IN_PROGRESS = -177,
         /** This operation would interfere with an existing subscription */
            _EXISTING_SUBSCRIPTION = -176,
        /** Assigned partitions (rebalance_cb) */
            _ASSIGN_PARTITIONS = -175,
        /** Revoked partitions (rebalance_cb) */
            _REVOKE_PARTITIONS = -174,
        /** Conflicting use */
            _CONFLICT = -173,
        /** Wrong state */
            _STATE = -172,
        /** Unknown protocol */
            _UNKNOWN_PROTOCOL = -171,
        /** Not implemented */
            _NOT_IMPLEMENTED = -170,
        /** Authentication failure*/
        _AUTHENTICATION = -169,
        /** No stored offset */
        _NO_OFFSET = -168,
        /** End internal error codes */
        _END = -100,

        /* Kafka broker errors: */
        /** Unknown broker error */
        UNKNOWN = -1,
        /** Success */
        NO_ERROR = 0,
        /** Offset out of range */
        OFFSET_OUT_OF_RANGE = 1,
        /** Invalid message */
        INVALID_MSG = 2,
        /** Unknown topic or partition */
        UNKNOWN_TOPIC_OR_PART = 3,
        /** Invalid message size */
        INVALID_MSG_SIZE = 4,
        /** Leader not available */
        LEADER_NOT_AVAILABLE = 5,
        /** Not leader for partition */
        NOT_LEADER_FOR_PARTITION = 6,
        /** Request timed out */
        REQUEST_TIMED_OUT = 7,
        /** Broker not available */
        BROKER_NOT_AVAILABLE = 8,
        /** Replica not available */
        REPLICA_NOT_AVAILABLE = 9,
        /** Message size too large */
        MSG_SIZE_TOO_LARGE = 10,
        /** StaleControllerEpochCode */
        STALE_CTRL_EPOCH = 11,
        /** Offset metadata string too large */
        OFFSET_METADATA_TOO_LARGE = 12,
        /** Broker disconnected before response received */
        NETWORK_EXCEPTION = 13,
        /** Group coordinator load in progress */
            GROUP_LOAD_IN_PROGRESS = 14,
         /** Group coordinator not available */
            GROUP_COORDINATOR_NOT_AVAILABLE = 15,
        /** Not coordinator for group */
            NOT_COORDINATOR_FOR_GROUP = 16,
        /** Invalid topic */
            TOPIC_EXCEPTION = 17,
        /** Message batch larger than configured server segment size */
            RECORD_LIST_TOO_LARGE = 18,
        /** Not enough in-sync replicas */
            NOT_ENOUGH_REPLICAS = 19,
        /** Message(s) written to insufficient number of in-sync replicas */
            NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,
        /** Invalid required acks value */
            INVALID_REQUIRED_ACKS = 21,
        /** Specified group generation id is not valid */
            ILLEGAL_GENERATION = 22,
        /** Inconsistent group protocol */
            INCONSISTENT_GROUP_PROTOCOL = 23,
        /** Invalid group.id */
        INVALID_GROUP_ID = 24,
        /** Unknown member */
            UNKNOWN_MEMBER_ID = 25,
        /** Invalid session timeout */
            INVALID_SESSION_TIMEOUT = 26,
        /** Group rebalance in progress */
        REBALANCE_IN_PROGRESS = 27,
        /** Commit offset data size is not valid */
            INVALID_COMMIT_OFFSET_SIZE = 28,
        /** Topic authorization failed */
            TOPIC_AUTHORIZATION_FAILED = 29,
        /** Group authorization failed */
        GROUP_AUTHORIZATION_FAILED = 30,
        /** Cluster authorization failed */
        CLUSTER_AUTHORIZATION_FAILED = 31
    };
}
