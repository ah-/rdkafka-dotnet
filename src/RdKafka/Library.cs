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

        public static int Version() => (int)rd_kafka_version();

        public static string VersionString() => Marshal.PtrToStringAnsi(rd_kafka_version_str());

        public static string[] DebugContexts => Marshal.PtrToStringAnsi(rd_kafka_get_debug_contexts()).Split(',');

        public static void SetLogLevel(int logLevel)
        {
            rd_kafka_set_log_level(IntPtr.Zero, (IntPtr) logLevel);
        }

        public static void WaitDestroyed(long timeoutMs)
        {
            if ((long) rd_kafka_wait_destroyed((IntPtr) timeoutMs) != 0)
            {
                throw new TimeoutException();
            }
        }
    }
}
