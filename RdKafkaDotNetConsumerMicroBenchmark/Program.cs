using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RdKafka;

namespace RdKafkaDotNetConsumerMicroBenchmark
{
    class Program
    {
        public static void Main(string[] args)
        {
            //string brokerList = "138.91.140.244";//d4
            string brokerList = "40.118.249.252";//b2
            //string brokerList = "localhost";
            List<String> topicList = new List<string>();
            topicList.Add("testBench24");
            var topics = topicList;

            var config = new Config() { GroupId = "simple-csharp-consumer" };
            DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            List<Double> latencies = new List<Double>();
            using (var consumer = new EventConsumer(config, brokerList))
            {
                consumer.OnMessage += (obj, msg) =>
                {
                    string text = Encoding.UTF8.GetString(msg.Payload, 0, msg.Payload.Length);
                    //Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {text}");
                    String receivedMessage = text;

                    String[] substrings = receivedMessage.Split('_');
                    String id = substrings[0];
                    String sentTimestamp = substrings[1];

                    TimeSpan diff = DateTime.Now.ToUniversalTime() - origin;
                    String receivedTimestamp = String.Format(Math.Floor(diff.TotalMilliseconds).ToString());

                    String finalMessage = String.Format(id + "_" + sentTimestamp + "_" + receivedTimestamp);
                    //Console.WriteLine("Payload on Consumer side == " + finalMessage);

                    Double latency = Convert.ToDouble(receivedTimestamp) - Convert.ToDouble(sentTimestamp);
                    //Console.WriteLine("Payload on Consumer side == " + finalMessage + ", latency == " + latency);

                    latencies.Add(latency);

                    //printing only on the nearest 100th device, since frequent reporting adds to latency
                    if (latencies.Count == 1000000)
                        PrintLatencyReport(latencies);
                    //Console.WriteLine("");
                    //Console.WriteLine(latencies.Count);
                };

                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topics.First(), 0, 5) });
                consumer.Start();

                Console.WriteLine("Started consumer, press enter to stop consuming");
                //Console.ReadLine();
                //PrintLatencyReport(latencies);
                Console.ReadLine();
            }
        }

        static void PrintLatencyReport(List<Double> latencies)
        {
            if (latencies.Count == 0)
                return;

            latencies.Sort();

            int length = latencies.Count;

            Console.WriteLine("Mean == " + latencies.Average());
            Console.WriteLine("Median == " + latencies[length / 2]);
            Console.WriteLine("50th Percentile == " + Percentile(latencies, 0.5));
            Console.WriteLine("95th Percentile == " + Percentile(latencies, 0.95));
            Console.WriteLine("99th Percentile == " + Percentile(latencies, 0.99));
        }

        static double Percentile(List<Double> sequence, double excelPercentile)
        {
            sequence.Sort();
            int N = sequence.Count;
            double n = (N - 1) * excelPercentile + 1;
            // Another method: double n = (N + 1) * excelPercentile;
            if (n == 1d) return sequence[0];
            else if (n == N) return sequence[N - 1];
            else
            {
                int k = (int)n;
                double d = n - k;
                return sequence[k - 1] + d * (sequence[k] - sequence[k - 1]);
            }
        }
    }
}
