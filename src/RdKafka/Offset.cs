
namespace RdKafka
{
    public static class Offset
    {
        /// <summary>
        /// Start consuming from beginning of kafka partition queue: oldest msg
        /// </summary>
        public static long Beginning = -2;

        /// <summary>
        /// Start consuming from end of kafka partition queue: next msg
        /// </summary>
        public static long End = -1;

        /// <summary>
        /// Start consuming from offset retrieved from offset store
        /// </summary>
        public static long Stored = -1000;

        /// <summary>
        /// Invalid offset
        /// </summary>
        public static long Invalid = -1001;
    }
}
