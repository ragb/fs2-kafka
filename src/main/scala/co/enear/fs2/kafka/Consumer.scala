package co.enear.fs2.kafka

import java.util.Collection
import scala.collection.JavaConverters._
import fs2._
import fs2.util.{ Async, Functor }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer._
import util.Properties

object Consumer {

  private[kafka] def createKafkaConsumer[F[_], K, V](
    settings: ConsumerSettings[K, V]
  )(implicit F: Async[F]): F[ConsumerControl[F, K, V]] = F.delay {
    new ConsumerControlImpl[F, K, V](
      new KafkaConsumer(Properties.fromMap(settings.properties), settings.keyDeserializer, settings.valueDeserializer), settings
    )
  }

  private[kafka] def makeConsumerRecordStream[F[_]: Async, K, V, O](settings: ConsumerSettings[K, V], subscription: Subscription)(use: ConsumerControl[F, K, V] => Stream[F, O]): Stream[F, O] = Stream.bracket(createKafkaConsumer[F, K, V](settings))({ consumer =>
    Stream.eval_(consumer.subscribe(subscription)) ++ use(consumer)
  }, _.close)

  def plainStream[F[_]: Async, K, V](settings: ConsumerSettings[K, V], Subscription: Subscription): Stream[F, ConsumerRecord[K, V]] = makeConsumerRecordStream[F, K, V, ConsumerRecord[K, V]](settings, Subscription) { consumer =>
    Stream.eval(consumer.poll)
      .flatMap(Stream.emits _)
      .repeat
  }

  def commitableMessageStream[F[_]: Async, K, V](settings: ConsumerSettings[K, V], subscription: Subscription): Stream[F, CommitableMessage[F, ConsumerRecord[K, V]]] = makeConsumerRecordStream(settings, subscription) { (consumer: ConsumerControl[F, K, V]) =>
    val messageBuilder = new CommitableMessageBuilder[F, K, V](consumer, settings)
    Stream.eval(consumer.poll)
      .flatMap(Stream.emits _)
      .map(messageBuilder.build _)
      .repeat
  }
}

trait ConsumerControl[F[_], K, V] {
  def close: F[Unit]

  def subscribe(subscription: Subscription): F[Unit]
  def poll: F[Seq[ConsumerRecord[K, V]]]
  def commiter: Commiter[F]
}

case class CommitableMessage[F[_], A](msg: A, commitableOffset: CommitableOffset[F])

object CommitableMessage {

  implicit def commitableMessageFunctor[F[_]] = new Functor[CommitableMessage[F, ?]] {
    override def map[A, B](fa: CommitableMessage[F, A])(f: A => B) = fa.copy[F, B](msg = f(fa.msg))
  }
}

final case class GroupTopicPartition(group: String, topic: String, partition: Int)

final case class PartitionOfsset(key: GroupTopicPartition, offset: Long)

trait Commitable[F[_]] {
  def commit: F[Unit]
}

trait CommitableOffset[F[_]] extends Commitable[F] {
  def partitionOffset: PartitionOfsset
}

trait Commiter[F[_]] {
  def commit(partitions: Map[TopicPartition, Long]): F[Unit]
}

private[kafka] class ConsumerControlImpl[F[_], K, V](consumer: Consumer[K, V], settings: ConsumerSettings[K, V])(implicit F: Async[F]) extends ConsumerControl[F, K, V] {

  override def close = F.delay { consumer.close() }
  override def subscribe(subscription: Subscription) = subscription match {
    case Subscription.AutoSubscription(topics, onAssigned, onRevoked) => (onAssigned, onRevoked) match {
      case (None, None) => F.delay { consumer.subscribe(topics.asJava) }
      case _ => F.delay {
        consumer.subscribe(topics.asJava, new ConsumerRebalanceListener {
          override def onPartitionsAssigned(partitions: Collection[TopicPartition]) = onAssigned foreach (_(consumer)(partitions.asScala))
          override def onPartitionsRevoked(partitions: Collection[TopicPartition]) = onRevoked foreach (_(consumer)(partitions.asScala))
        })
      }
    }
    case Subscription.ManualAssignment(topicsMap) =>
      val assignments = topicsMap.toSeq.map(t => new TopicPartition(t._1, t._2)).asJava
      F.delay {
        consumer.assign(assignments)
      }
  }

  override def poll = F.delay {
    consumer.poll(settings.pollInterval.toMillis).iterator.asScala.toSeq
  }


  override def commiter = new Commiter[F] {
    override def commit(partitionsAndOffsets: Map[TopicPartition, Long]) = F.delay {
      consumer.commitSync(partitionsAndOffsets.mapValues(offset => new OffsetAndMetadata(offset)).asJava)
()
    }
  //   override def commit(partitionsAndoffsets: Map[TopicPartition, Long]) = F.async { register =>
  //     F.delay {
  //       consumer.commitAsync(partitionsAndoffsets.mapValues(offset => new OffsetAndMetadata(offset)).asJava, new OffsetCommitCallback {
  //         override def onComplete(offsets: java.util.Map[TopicPartition, OffsetAndMetadata], exception: Exception) = {
  //           if (offsets != null) register(Right(()))
  //           else register(Left(exception))
  //         }
  //       })
  //       ()
  //     }
  //   }
   }
}

private[kafka] class CommitableMessageBuilder[F[_], K, V](consumer: ConsumerControl[F, K, V], settings: ConsumerSettings[K, V])(implicit F: Async[F]) {
  def build(record: ConsumerRecord[K, V]): CommitableMessage[F, ConsumerRecord[K, V]] = {
    val offset = PartitionOfsset(GroupTopicPartition(settings.properties(ConsumerConfig.GROUP_ID_CONFIG), record.topic(), record.partition()), record.offset())
    val commitableOffset = new CommitableOffset[F] {
      override val partitionOffset = offset
      override def commit = consumer.commiter.commit(Map(new TopicPartition(partitionOffset.key.topic, partitionOffset.key.partition) -> (offset.offset + 1)))
    }
    CommitableMessage[F, ConsumerRecord[K, V]](record, commitableOffset)
  }
}
