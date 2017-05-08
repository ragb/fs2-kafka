package co.enear.fs2.kafka

import internal._
import scala.collection.JavaConverters._
import fs2._
import fs2.util.{ Async, Functor }
import fs2.util.syntax._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer._
import util.Properties

trait Consumer[F[_], K, V] {

  private[kafka] def createConsumer: F[ConsumerControl[F, K, V]]

  trait StreamType {
    type OutStreamType[_] <: Stream[F, _]
    private[kafka] def makeStream(
      subscription: Subscription,
      builder: MessageBuilder[F, K, V]
    )(implicit F: Async[F]): OutStreamType[builder.Message]

    def plainMessages(
      subscription: Subscription
    )(implicit F: Async[F]): OutStreamType[ConsumerRecord[K, V]] = makeStream(subscription, new PlainMessageBuilder[F, K, V])
    def commitableMessages(
      subscription: Subscription
    )(implicit F: Async[F]): OutStreamType[CommitableMessage[F, ConsumerRecord[K, V]]] = makeStream(subscription, new CommitableMessageBuilder[F, K, V])
  }
  val simpleStream = new StreamType {
    type OutStreamType[A] = Stream[F, A]
    private[kafka] def makeStream(
      subscription: Subscription,
      builder: MessageBuilder[F, K, V]
    )(implicit F: Async[F]): OutStreamType[builder.Message] = Stream.bracket(createConsumer)({ consumer =>
      val subscriber = new SimpleSubscriber[F, K, V]
      Stream.eval_(subscriber.subscribe(consumer)(subscription)) ++
        consumer.pollStream
        .flatMap(consumerRecords => Stream.emits(consumerRecords.iterator.asScala.toSeq))
        .through(builder.build(consumer)(_))
    }, _.close)
  }

  val partitionedStreams = new StreamType {
    type OutStreamType[A] = Stream[F, (TopicPartition, Stream[F, A])]

    private[kafka] def makeStream(
      subscription: Subscription,
      builder: MessageBuilder[F, K, V]
    )(implicit F: Async[F]): Stream[F, (TopicPartition, Stream[F, builder.Message])] =
      Stream.bracket(createConsumer)({ consumer: ConsumerControl[F, K, V] =>
        Stream.eval(PartitionedStreamManager[F, K, V](consumer))
          .flatMap { manager =>
            manager.partitionedStream(subscription, builder)
          }
      }, _.close)

  }

}

object Consumer {

  def apply[F[+_], K, V](
    settings: ConsumerSettings[K, V]
  )(implicit F: Async[F]): Consumer[F, K, V] = {
    val create = async.mutable.Queue.unbounded[F, F[Unit]].map { tasksQueue =>
      new ConsumerControlImpl[F, K, V](
        new KafkaConsumer(Properties.fromMap(settings.properties), settings.keyDeserializer, settings.valueDeserializer), settings, tasksQueue
      )
    }
    new Consumer[F, K, V] {
      override val createConsumer = create
    }
  }

}

trait ConsumerControl[F[_], K, V] {
  def settings: ConsumerSettings[K, V]
  def close: F[Unit]
  def assignment: F[Set[TopicPartition]]
  def subscription: F[Set[String]]
  def assign(assignment: ManualSubscription): F[Unit]
  def subscribe(subscription: AutoSubscription, listener: ConsumerRebalanceListener): F[Unit]
  def poll: F[ConsumerRecords[K, V]]
  def pollStream: Stream[F, ConsumerRecords[K, V]]
  def pause(partitions: Set[TopicPartition]): F[Unit]
  def resume(partitions: Set[TopicPartition]): F[Unit]
  def wakeup: F[Unit]
  def commiter: Commiter[F]

  private[kafka] def rawConsumer: org.apache.kafka.clients.consumer.Consumer[K, V]
}

final case class CommitableMessage[F[_], A](msg: A, commitableOffset: CommitableOffset[F]) {
  def map[B](f: A => B) = copy[F, B](msg = f(msg))
}

object CommitableMessage extends CommitableMessageInstances

private[kafka] trait CommitableMessageInstances {

  implicit def commitableMessageFunctor[F[_]]: Functor[CommitableMessage[F, ?]] = new Functor[CommitableMessage[F, ?]] {
    override def map[A, B](fa: CommitableMessage[F, A])(f: A => B) = fa map f
  }

}

final case class GroupTopicPartition(group: String, topic: String, partition: Int)

final case class PartitionOfsset(key: GroupTopicPartition, offset: Long)

final case class Commited(partitionOffsets: Map[TopicPartition, Long])

trait Commitable[F[_]] {
  def commit: F[Commited]
}

trait CommitableOffset[F[_]] extends Commitable[F] {
  def partitionOffset: PartitionOfsset
}

trait Commiter[F[_]] {
  def commit(partitions: Map[TopicPartition, Long]): F[Commited]
}

