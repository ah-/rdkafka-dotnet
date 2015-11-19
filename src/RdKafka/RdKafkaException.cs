using System;
using System.Runtime.InteropServices;

namespace RdKafka
{
    public class RdKafkaException : Exception
    {
        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr rd_kafka_err2str(ErrorCode err);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_errno2err(IntPtr errno);

        public RdKafkaException(string message, ErrorCode errorCode)
            : base(message)
        {
            ErrorCode = errorCode;
        }

        internal static string ErrorToString(ErrorCode errorCode)
        {
            return Marshal.PtrToStringAnsi(rd_kafka_err2str(errorCode));
        }

        internal static RdKafkaException FromErrNo(IntPtr errno, string message)
        {
            return FromErr(rd_kafka_errno2err(errno), message);
        }

        internal static RdKafkaException FromErr(ErrorCode err, string message)
        {
            var errorMessage = $"Error {err} - {ErrorToString(err)}";
            if (message == null)
            {
                return new RdKafkaException(errorMessage, err);
            }
            else
            {
                return new RdKafkaException($"{message} ({errorMessage})", err);
            }
        }

        public ErrorCode ErrorCode { get; }
    }
}
