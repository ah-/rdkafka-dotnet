using System.Collections.Generic;
using RdKafka.Internal;

namespace RdKafka
{
    public class Config
    {
        internal readonly SafeConfigHandle handle;

        public Config()
        {
            handle = SafeConfigHandle.Create();
        }

        public Dictionary<string, string> Dump() => handle.Dump();

        public string this[string name]
        {
            set
            {
                handle.Set(name, value);
            }
            get
            {
                return handle.Get(name);
            }
        }

        // Helpers for common config options
        public string GroupId
        {
            set { this["group.id"] = value; }
            get { return this["group.id"]; }
        }

        public bool EnableAutoCommit
        {
            set { this["enable.auto.commit"] = value ? "true" : "false"; }
            get { return this["enable.auto.commit"] == "true"; }
        }
    }
}
