using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Jaeger;
using Jaeger.Samplers;
using Microsoft.Extensions.Logging.Abstractions;
using OpenTracing;
using OpenTracing.Confluent.Kafka;

namespace Confluent.Kafka.Clients.Traced.By.Jaeger
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            try
            {
                var tracer = GetTracer();

                var tracingProducer = GetTracingProducer(tracer);
                var message = new Message<string, string> { Headers = new Headers() };
                message.Key = "TestMessage";
                message.Value = "{}";
                message.Headers.Add("processInstanceId", Encoding.UTF8.GetBytes("111123321"));
                message.Headers.Add("clientType", Encoding.UTF8.GetBytes("PF"));
                message.Headers.Add("authorizationTicket", Encoding.UTF8.GetBytes("OUACK"));
                message.Headers.Add("correlationId", Encoding.UTF8.GetBytes("QUACK"));
                var produceResult = await tracingProducer.ProduceAsync("ro.btrl.in.copo.adapter.clientenrolment.createCustomer.v1", message);

                Console.WriteLine(
                    $"Sent message to partition {produceResult.Partition.Value}, offset {produceResult.Offset.Value} on topic {produceResult.Topic}");

                var tracingConsumer = GetTracingConsumer(tracer);
                tracingConsumer.Subscribe("ro.btrl.out.copo.adapter.clientenrolment.createCustomer.v1");

                const int maxConsumeAttempts = 10;
                var attemptsTried = 0;
               
                while (attemptsTried < maxConsumeAttempts)
                {
                    using (var scope = tracingConsumer.Consume(TimeSpan.FromSeconds(5), out var consumeResult))
                    {
                        attemptsTried++;
                        if (consumeResult == null)
                            continue;

                        scope.Span.SetTag("Consumer.Timeout", 5);
                        scope.Span.SetTag("Consumer.Message.Key", consumeResult.Message.Key);
                        scope.Span.SetTag("Consumer.Message.Value", consumeResult.Message.Value);

                        // Simulate consumption load for nicer traces
                        await Task.Delay(TimeSpan.FromSeconds(1));

                        Console.WriteLine(
                            $"Received message from partition {consumeResult.Partition.Value}, offset {consumeResult.Offset.Value} on topic {consumeResult.Topic}");

                        break;
                    }
                }

                if (attemptsTried == maxConsumeAttempts)
                {
                    Console.WriteLine("Never received any messages");
                }

                tracingProducer.Dispose();
                tracingConsumer.Dispose();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            Console.WriteLine("Press any key to exit");
            Console.ReadKey();
        }

        private static ITracer GetTracer()
        {
            var logFactory = new NullLoggerFactory();

            return new Configuration("kafka.jaeger.example", logFactory)
                .WithReporter(new Configuration.ReporterConfiguration(logFactory)
                    .WithSender(new Configuration.SenderConfiguration(logFactory)
                        .WithEndpoint("http://jaeger:14268/api/traces")))
                .WithSampler(new Configuration.SamplerConfiguration(logFactory)
                    .WithType(ConstSampler.Type)
                    .WithParam(1))
                .GetTracer();
        }

        private static ClientConfig GetClientConfig()
        {
            return new ClientConfig
            {
                 BootstrapServers = "127.0.0.1:9092"
            };
        }

        private static TracingConsumer<string, string> GetTracingConsumer(ITracer tracer)
        {
            var consumerConfig = new ConsumerConfig(GetClientConfig())
            {
                AutoOffsetReset = AutoOffsetReset.Latest,
                GroupId = "kafka.jaeger"
            };

            var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
            var tracingConsumer = new TracingConsumer<string, string>(tracer, consumer);
           
            return tracingConsumer;
        }

        private static TracingProducer<string, string> GetTracingProducer(ITracer tracer)
        {
            var producerConfig = new ProducerConfig(GetClientConfig())
            {
                MessageTimeoutMs = 5000
            };
            var producer = new ProducerBuilder<string, string>(producerConfig).Build();
            var tracingProducer = new TracingProducer<string, string>(tracer, producer);
            

            return tracingProducer;
        }
    }
}
