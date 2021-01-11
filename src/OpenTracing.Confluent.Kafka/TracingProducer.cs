using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using OpenTracing.Tag;

namespace OpenTracing.Confluent.Kafka
{
    public class TracingProducer<TKey, TValue> : IProducer<TKey, TValue>
    {
        private readonly ITracer _tracer;
        private readonly IProducer<TKey, TValue> _producer;

        public TracingProducer(ITracer tracer, IProducer<TKey, TValue> producer)
        {
            _tracer = tracer;
            _producer = producer;
        }

        public void Dispose()
        {
            _producer.Dispose();
        }

        public int AddBrokers(string brokers)
        {
            return _producer.AddBrokers(brokers);
        }

        public Handle Handle => _producer.Handle;

        public string Name => _producer.Name;

        public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(string topic, Message<TKey, TValue> message,
            CancellationToken cancellationToken = new CancellationToken())
        {
            message.Headers = message.Headers ?? new Headers();

            using (var scope = _tracer.CreateAndInjectActiveProducerScopeFrom(
                message.Headers.ToDictionary(Encoding.UTF8)))
            {
                scope.Span.SetTag(Tags.MessageBusDestination, topic);

                var report = await _producer.ProduceAsync(topic, message, cancellationToken);

                scope.Span.SetTag("kafka.topic", report.Topic);
                scope.Span.SetTag("kafka.partition", report.Partition);
                scope.Span.SetTag("kafka.offset", report.Offset);

                return report;
            }
        }

        public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(TopicPartition topicPartition, Message<TKey, TValue> message,
            CancellationToken cancellationToken = new CancellationToken())
        {
            message.Headers = message.Headers ?? new Headers();

            using (var scope = _tracer.CreateAndInjectActiveProducerScopeFrom(message.Headers.ToDictionary(Encoding.UTF8)))
            {
                scope.Span.SetTag(Tags.MessageBusDestination, topicPartition.Topic);

                var report = await _producer.ProduceAsync(topicPartition, message, cancellationToken);

                scope.Span.SetTag("kafka.topic", report.Topic);
                scope.Span.SetTag("kafka.partition", report.Partition);
                scope.Span.SetTag("kafka.offset", report.Offset);

                return report;
            }
        }

        public int Poll(TimeSpan timeout)
        {
            return _producer.Poll(timeout);
        }

        public int Flush(TimeSpan timeout)
        {
            return _producer.Flush(timeout);
        }

        public void Flush(CancellationToken cancellationToken = new CancellationToken())
        {
            _producer.Flush(cancellationToken);
        }

        public void Produce(string topic, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            _producer.Produce(topic, message, deliveryHandler);
        }

        public void Produce(TopicPartition topicPartition, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            _producer.Produce(topicPartition, message, deliveryHandler);
        }

        public void InitTransactions(TimeSpan timeout)
        {
            _producer.InitTransactions(timeout);
        }

        public void BeginTransaction()
        {
            _producer.BeginTransaction();
        }

        public void CommitTransaction(TimeSpan timeout)
        {
            _producer.CommitTransaction(timeout);
        }

        public void AbortTransaction(TimeSpan timeout)
        {
            _producer.AbortTransaction(timeout);
        }

        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
        {
            _producer.SendOffsetsToTransaction(offsets, groupMetadata, timeout);
        }
    }
}