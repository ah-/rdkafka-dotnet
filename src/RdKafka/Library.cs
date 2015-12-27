using System;
using System.Runtime.InteropServices;
using RdKafka.Internal;

namespace RdKafka
{
    public static class Library
    {
        public delegate void LogCallback(string handle, int level, string fac, string buf);

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
        public static int Version => (int) LibRdKafka.version();

        /// <summary>
        /// The librdkafka version as string.
        /// </summary>
        public static string VersionString =>
            Marshal.PtrToStringAnsi(LibRdKafka.version_str());

        /// <summary>
        /// List of the supported debug contexts.
        /// </summary>
        public static string[] DebugContexts =>
            Marshal.PtrToStringAnsi(LibRdKafka.get_debug_contexts()).Split(',');

        public static void SetLogLevel(int logLevel)
        {
            LibRdKafka.set_log_level(IntPtr.Zero, (IntPtr) logLevel);
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
            if ((long) LibRdKafka.wait_destroyed((IntPtr) timeoutMs) != 0)
            {
                throw new TimeoutException();
            }
        }
    }
}
