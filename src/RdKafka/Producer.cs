using System;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using RdKafka.Internal;

namespace RdKafka
{
    public class Producer : Handle, IDisposable
    {
        readonly Task callbackTask;
        readonly CancellationTokenSource callbackCts;

        public Producer(string brokerList) : this(null, brokerList) {}

        public Producer(Config config, string brokerList = null)
        {
            config = config ?? new Config();
            LibRdKafka.rd_kafka_conf_set_dr_msg_cb(config.handle.DangerousGetHandle(),
                    DeliveryReportDelegate);
            handle = SafeKafkaHandle.Create(RdKafkaType.Producer, config.handle);

            if (brokerList != null)
            {
                handle.AddBrokers(brokerList);
            }

            callbackCts = new CancellationTokenSource();
            callbackTask = StartCallbackTask(callbackCts.Token);
        }

        public Topic Topic(string topic, TopicConfig config = null)
        {
            config = config ?? new TopicConfig();
            // TODO: default to this in a better way
            config["produce.offset.report"] = "true";
            return new Topic(handle.Topic(topic, config), this);
        }

        public void Dispose()
        {
            callbackCts.Cancel();
            callbackTask.Wait();

            // Wait until all outstanding sends have completed
            while (OutQueueLength > 0)
            {
                handle.Poll((IntPtr) 100);
            }

            handle.Dispose();
        }

        // Explicitly keep reference to delegate so it stays alive
        static LibRdKafka.DeliveryReportCallback DeliveryReportDelegate = DeliveryReportCallback;

        static void DeliveryReportCallback(IntPtr rk,
                ref rd_kafka_message rkmessage, IntPtr opaque)
        {
            // msg_opaque was set by Topic.Produce
            var gch = GCHandle.FromIntPtr(rkmessage._private);
            var deliveryCompletionSource = (TaskCompletionSource<DeliveryReport>) gch.Target;
            gch.Free();

            if (rkmessage.err != 0)
            {
                deliveryCompletionSource.SetException(
                    RdKafkaException.FromErr(
                        rkmessage.err,
                        Marshal.PtrToStringAnsi(rkmessage.payload)));
            }

            deliveryCompletionSource.SetResult(new DeliveryReport() {
                Offset = rkmessage.offset,
                Partition = rkmessage.partition
            });
        }

        Task StartCallbackTask(CancellationToken ct)
        {
            return Task.Factory.StartNew(() =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        handle.Poll((IntPtr) 1000);
                    }
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }
    }
}
