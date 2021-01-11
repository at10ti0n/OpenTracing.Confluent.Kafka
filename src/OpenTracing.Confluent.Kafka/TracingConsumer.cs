using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Confluent.Kafka;

namespace OpenTracing.Confluent.Kafka
{
    public class TracingConsumer<TKey, TValue> : IConsumer<TKey, TValue>
    {
        private readonly ITracer _tracer;
        private readonly IConsumer<TKey, TValue> _consumer;

        public TracingConsumer(ITracer tracer, IConsumer<TKey, TValue> consumer)
        {
            _tracer = tracer;
            _consumer = consumer;
        }

        public void Dispose()
        {
            _consumer.Dispose();
        }

        public int AddBrokers(string brokers)
        {
            return _consumer.AddBrokers(brokers);
        }

        public Handle Handle => _consumer.Handle;
        public string Name => _consumer.Name;

        public ConsumeResult<TKey, TValue> Consume(TimeSpan timeout)
        {
            return _consumer.Consume(timeout);
        }

        public IScope Consume(TimeSpan timeout, out ConsumeResult<TKey, TValue> result)
        {
            result = _consumer.Consume(timeout);

            if (result == null)
            {
                return null;
            }

            result.Message.Headers = result.Message.Headers ?? new Headers();

            var scope = _tracer.CreateAndInjectActiveConsumerScopeFrom(result.Message.Headers.ToDictionary(Encoding.UTF8));

            scope.Span.SetTag("kafka.topic", result.Topic);
            scope.Span.SetTag("kafka.partition", result.Partition);
            scope.Span.SetTag("kafka.offset", result.Offset);
            return scope;
        }
        
        public ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken)
        {
            return _consumer.Consume(cancellationToken);
        }

        public IScope Consume(CancellationToken cancellationToken, out ConsumeResult<TKey, TValue> result)
        {
            result = _consumer.Consume(cancellationToken);

            if (result == null)
            {
                return null;
            }

            result.Message.Headers = result.Message.Headers ?? new Headers();

            var scope = _tracer.CreateAndInjectActiveConsumerScopeFrom(result.Message.Headers.ToDictionary(Encoding.UTF8));

            scope.Span.SetTag("kafka.topic", result.Topic);
            scope.Span.SetTag("kafka.partition", result.Partition);
            scope.Span.SetTag("kafka.offset", result.Offset);
            return scope;
        }

        public void Subscribe(IEnumerable<string> topics)
        {
            _consumer.Subscribe(topics);
        }

        public void Subscribe(string topic)
        {
            _consumer.Subscribe(topic);
        }

        public void Unsubscribe()
        {
            _consumer.Unsubscribe();
        }

        public void Assign(TopicPartition partition)
        {
            _consumer.Assign(partition);
        }

        public void Assign(TopicPartitionOffset partition)
        {
            _consumer.Assign(partition);
        }

        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
        {
            _consumer.Assign(partitions);
        }

        public void Assign(IEnumerable<TopicPartition> partitions)
        {
            _consumer.Assign(partitions);
        }

        public void Unassign()
        {
            _consumer.Unassign();
        }

        public void StoreOffset(ConsumeResult<TKey, TValue> result)
        {
            _consumer.StoreOffset(result);
        }

        public List<TopicPartitionOffset> Commit()
        {
            return _consumer.Commit();
        }

        public void Commit(ConsumeResult<TKey, TValue> result)
        {
            _consumer.Commit(result);
        }

        public void Commit(IEnumerable<TopicPartitionOffset> offsets)
        {
            _consumer.Commit(offsets);
        }

        public void Seek(TopicPartitionOffset tpo)
        {
            _consumer.Seek(tpo);
        }

        public void Pause(IEnumerable<TopicPartition> partitions)
        {
            _consumer.Pause(partitions);
        }

        public void Resume(IEnumerable<TopicPartition> partitions)
        {
            _consumer.Resume(partitions);
        }

        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
        {
            return _consumer.Committed(partitions, timeout);
        }

        public Offset Position(TopicPartition partition)
        {
            return _consumer.Position(partition);
        }

        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
        {
            return _consumer.OffsetsForTimes(timestampsToSearch, timeout);
        }

        public ConsumeResult<TKey, TValue> Consume(int millisecondsTimeout)
        {
            return _consumer.Consume(millisecondsTimeout);
        }

        public void StoreOffset(TopicPartitionOffset offset)
        {
            _consumer.StoreOffset(offset);
        }

        public List<TopicPartitionOffset> Committed(TimeSpan timeout)
        {
            return _consumer.Committed(timeout);
        }

        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
        {
            return _consumer.GetWatermarkOffsets(topicPartition);
        }

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
        {
            return _consumer.QueryWatermarkOffsets(topicPartition, timeout);
        }

        public void Close()
        {
            _consumer.Close();
        }
        public string MemberId => _consumer.MemberId;
        public List<TopicPartition> Assignment => _consumer.Assignment;
        public List<string> Subscription => _consumer.Subscription;

        public IConsumerGroupMetadata ConsumerGroupMetadata => _consumer.ConsumerGroupMetadata;
    }
}
