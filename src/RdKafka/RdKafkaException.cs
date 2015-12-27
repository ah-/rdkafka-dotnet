using System;
using System.Runtime.InteropServices;
using RdKafka.Internal;

namespace RdKafka
{
    public class RdKafkaException : Exception
    {
        public RdKafkaException(string message, ErrorCode errorCode)
            : base(message)
        {
            ErrorCode = errorCode;
        }

        internal static string ErrorToString(ErrorCode errorCode)
        {
            return Marshal.PtrToStringAnsi(LibRdKafka.err2str(errorCode));
        }

        internal static RdKafkaException FromErrNo(IntPtr errno, string message)
        {
            return FromErr(LibRdKafka.errno2err(errno), message);
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
