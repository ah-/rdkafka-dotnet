using System;
using System.IO;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Copy.Librdkafka
{
    public class Program
    {
        public void Main(string[] args)
        {
            try
            {
                var packagesFolder = Environment.GetEnvironmentVariable("DNX_PACKAGES");

                if (string.IsNullOrEmpty(packagesFolder))
                {
                    var dnxFolder = Environment.GetEnvironmentVariable("DNX_HOME") ??
                                    Environment.GetEnvironmentVariable("DNX_USER_HOME") ??
                                    Environment.GetEnvironmentVariable("DNX_GLOBAL_HOME");

                    var firstCandidate = dnxFolder?.Split(';')
                                                  ?.Select(path => Environment.ExpandEnvironmentVariables(path))
                                                  ?.Where(path => Directory.Exists(path))
                                                  ?.FirstOrDefault();

                    if (string.IsNullOrEmpty(firstCandidate))
                    {
                        dnxFolder = Path.Combine(GetHome(), ".dnx");
                    }
                    else
                    {
                        dnxFolder = firstCandidate;
                    }

                    packagesFolder = Path.Combine(dnxFolder, "packages");
                }

                packagesFolder = Environment.ExpandEnvironmentVariables(packagesFolder);

                var lockJson = JObject.Parse(File.ReadAllText("project.lock.json"));

                foreach (var librdkafkaLib in lockJson["libraries"].OfType<JProperty>().Where(
                    p => p.Name.StartsWith("RdKafka.Internal.librdkafka", StringComparison.Ordinal)))
                {
                    foreach (var filePath in librdkafkaLib.Value["files"].Select(v => v.Value<string>()))
                    {
                        if (filePath.ToString().StartsWith("runtimes/", StringComparison.Ordinal))
                        {
                            Directory.CreateDirectory(Path.GetDirectoryName(filePath));
                            File.Copy(Path.Combine(packagesFolder, librdkafkaLib.Name, filePath), filePath, overwrite: true);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }
        }

        private string GetHome()
        {
#if DNX451
            return Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
#else
            return Environment.GetEnvironmentVariable("HOME") ??
                Environment.GetEnvironmentVariable("USERPROFILE") ??
                Environment.GetEnvironmentVariable("HOMEDRIVE") + Environment.GetEnvironmentVariable("HOMEPATH");
#endif
        }
    }
}
