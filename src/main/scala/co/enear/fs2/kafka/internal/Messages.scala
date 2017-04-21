package co.enear.fs2.kafka
package internal

import fs2._

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

private[kafka] trait MessageBuilder[F[_], K, V] {
  type Message
  def build(consumer: ConsumerControl[F, K, V]): Pipe[F, ConsumerRecord[K, V], Message]
}

private[kafka] class PlainMessageBuilder[F[_], K, V] extends MessageBuilder[F, K, V] {
  type Message = ConsumerRecord[K, V]
  override def build(consumer: ConsumerControl[F, K, V]) = pipe.id
}

private[kafka] class CommitableMessageBuilder[F[_], K, V]() extends MessageBuilder[F, K, V] {
  type Message = CommitableMessage[F, ConsumerRecord[K, V]]
  override def build(consumer: ConsumerControl[F, K, V]): Pipe[F, ConsumerRecord[K, V], Message] = _.map { record =>
    val offset = PartitionOfsset(GroupTopicPartition(consumer.settings.properties(ConsumerConfig.GROUP_ID_CONFIG), record.topic(), record.partition()), record.offset())
    val commitableOffset = new CommitableOffset[F] {
      override val partitionOffset = offset
      override def commit = consumer.commiter.commit(Map(new TopicPartition(partitionOffset.key.topic, partitionOffset.key.partition) -> (offset.offset + 1)))
    }
    CommitableMessage[F, ConsumerRecord[K, V]](record, commitableOffset)
  }
}
