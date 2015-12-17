using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace RdKafka.Internal
{
    enum ConfRes {
        Unknown = -2, /* Unknown configuration name. */
        Invalid = -1, /* Invalid configuration value. */
        Ok = 0        /* Configuration okay */
    }

    class SafeConfigHandle : SafeHandleZeroIsInvalid
    {
        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern SafeConfigHandle rd_kafka_conf_new();

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern void rd_kafka_conf_destroy(IntPtr conf);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern ConfRes rd_kafka_conf_set(
                IntPtr conf,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                [MarshalAs(UnmanagedType.LPStr)] string value,
                StringBuilder errstr,
                UIntPtr errstr_size);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern ConfRes rd_kafka_conf_get(
                IntPtr conf,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                StringBuilder dest, ref UIntPtr dest_size);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern /* const char ** */ IntPtr rd_kafka_conf_dump(
                IntPtr conf, /* size_t * */ out UIntPtr cntp);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern void rd_kafka_conf_dump_free(/* const char ** */ IntPtr arr, UIntPtr cnt);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern /* rd_kafka_conf_t * */ IntPtr rd_kafka_conf_dup(IntPtr conf);

        private SafeConfigHandle()
        {
        }

        internal static SafeConfigHandle Create()
        {
            var ch = rd_kafka_conf_new();
            if (ch.IsInvalid)
            {
                throw new Exception("Failed to create config");
            }
            return ch;
        }

        protected override bool ReleaseHandle()
        {
            rd_kafka_conf_destroy(handle);
            return true;
        }

        internal IntPtr Dup()
        {
            return rd_kafka_conf_dup(handle);
        }

        internal Dictionary<string, string> Dump()
        {
            UIntPtr cntp = (UIntPtr) 0;
            IntPtr data = rd_kafka_conf_dump(handle, out cntp);

            if (data == IntPtr.Zero)
            {
                throw new Exception("Zero data");
            }

            try
            {
                if (((int) cntp & 1) != 0)
                {
                    // Expect Key -> Value, so even number of strings
                    throw new Exception("Invalid number of config entries");
                }

                var dict = new Dictionary<string, string>();
                for (int i = 0; i < (int) cntp / 2; i++)
                {
                    dict.Add(Marshal.PtrToStringAnsi(Marshal.ReadIntPtr(data, 2 * i * Marshal.SizeOf<IntPtr>())),
                             Marshal.PtrToStringAnsi(Marshal.ReadIntPtr(data, (2 * i + 1) * Marshal.SizeOf<IntPtr>())));
                }
                // Filter out callback pointers
                return dict.Where(kv => !kv.Key.EndsWith("_cb")).ToDictionary(kv => kv.Key, kv => kv.Value);
            }
            finally
            {
                rd_kafka_conf_dump_free(data, cntp);
            }
        }

        internal void Set(string name, string value)
        {
            // TODO: Constant instead of 512?
            var errorStringBuilder = new StringBuilder(512);
            ConfRes res = rd_kafka_conf_set(handle, name, value,
                    errorStringBuilder, (UIntPtr) errorStringBuilder.Capacity);
            if (res == ConfRes.Ok)
            {
                return;
            }
            else if (res == ConfRes.Invalid)
            {
                throw new InvalidOperationException(errorStringBuilder.ToString());
            }
            else if (res == ConfRes.Unknown)
            {
                throw new InvalidOperationException(errorStringBuilder.ToString());
            }
            else
            {
                throw new Exception("Unknown error while setting configuration property");
            }
        }

        internal string Get(string name)
        {
            UIntPtr destSize = (UIntPtr) 0;
            StringBuilder sb = null;

            ConfRes res = rd_kafka_conf_get(handle, name, null, ref destSize);
            if (res == ConfRes.Ok)
            {
                sb = new StringBuilder((int) destSize);
                res = rd_kafka_conf_get(handle, name, sb, ref destSize);
            }
            if (res != ConfRes.Ok)
            {
                if (res == ConfRes.Unknown)
                {
                    throw new InvalidOperationException($"No such configuration property: {name}");
                }
                throw new Exception("Unknown error while getting configuration property");
            }
            return sb?.ToString();
        }
    }
}
