using System;
using System.Runtime.InteropServices;
using System.Text;

namespace RdKafka.Internal
{
    internal static class PlatformApis
    {
        static PlatformApis()
        {
            bool isMono = Type.GetType("Mono.Runtime") != null;
            if (isMono)
            {
                // When running on Mono in Darwin OSVersion doesn't return Darwin. It returns Unix instead.
                // Fallback to use uname.
                IsDarwinMono = string.Equals(GetUname(), "Darwin", StringComparison.Ordinal);
            }
        }

        internal static bool IsDarwinMono = false;

        [DllImport("libc")]
        static extern int uname(StringBuilder buf);

        static string GetUname()
        {
            try
            {
                var stringBuilder = new StringBuilder(8192);
                if (uname(stringBuilder) == 0)
                {
                    return stringBuilder.ToString();
                }
                return string.Empty;
            }
            catch
            {
                return string.Empty;
            }
        }
    }
}
