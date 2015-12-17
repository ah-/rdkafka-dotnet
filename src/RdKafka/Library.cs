using System;
using System.Runtime.InteropServices;

namespace RdKafka
{
    public static class Library
    {
        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr rd_kafka_version();

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr rd_kafka_version_str();

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr rd_kafka_get_debug_contexts();

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern void rd_kafka_set_log_level(IntPtr rk, IntPtr level);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr rd_kafka_wait_destroyed(IntPtr timeout_ms);

        /// <summary>
        /// Returns the librdkafka version as integer.
        ///
        /// Interpreted as hex MM.mm.rr.xx:
        ///  - MM = Major
        ///  - mm = minor
        ///  - rr = revision
        ///  - xx = pre-release id (0xff is the final release)
        ///
        /// E.g.: \c 0x000901ff = 0.9.1
        /// </summary>
        public static int Version => (int) rd_kafka_version();

        /// <summary>
        /// The librdkafka version as string.
        /// </summary>
        public static string VersionString => Marshal.PtrToStringAnsi(rd_kafka_version_str());

        /// <summary>
        /// List of the supported debug contexts.
        /// </summary>
        public static string[] DebugContexts => Marshal.PtrToStringAnsi(rd_kafka_get_debug_contexts()).Split(',');

        public static void SetLogLevel(int logLevel)
        {
            rd_kafka_set_log_level(IntPtr.Zero, (IntPtr) logLevel);
        }

        /// <summary>
        /// Wait for all rdkafka objects to be destroyed.
        ///
        /// Returns if all kafka objects are now destroyed,
        /// or throw TimeoutException if the timeout was reached.
        ///
        /// Since RdKafka handle deletion is an async operation the
        /// WaitDestroyed() function can be used for applications where
        /// a clean shutdown is required.
        /// </summary>
        public static void WaitDestroyed(long timeoutMs)
        {
            if ((long) rd_kafka_wait_destroyed((IntPtr) timeoutMs) != 0)
            {
                throw new TimeoutException();
            }
        }
    }
}
