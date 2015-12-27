using System;
using RdKafka;

namespace MsBuild.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            // Test project to ensure it is possible to use RdKafka in msbuild based projects

            Console.WriteLine($"Hello RdKafka from a MsBuild project!");
            Console.WriteLine($"IntPtr.Size: {IntPtr.Size}");
            Console.WriteLine($"{Library.Version:X} {Library.VersionString}");
            Console.ReadLine();
        }
    }
}
