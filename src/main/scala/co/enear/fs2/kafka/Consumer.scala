package co.enear.fs2.kafka

import fs2._
import fs2.util.{ Async, Functor }
import fs2.util.syntax._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer._
import util.Properties
import internal._

object Consumer {

  private[kafka] def createKafkaConsumer[F[_], K, V](
    settings: ConsumerSettings[K, V]
  )(implicit F: Async[F]): F[ConsumerControl[F, K, V]] = async.mutable.Queue.unbounded[F, F[Unit]].map { tasksQueue =>
    new ConsumerControlImpl[F, K, V](
      new KafkaConsumer(Properties.fromMap(settings.properties), settings.keyDeserializer, settings.valueDeserializer), settings, tasksQueue
    )
  }

  private[kafka] def makeConsumerRecordStream[F[_]: Async, K, V, O, S <: Subscription](settings: ConsumerSettings[K, V], subscription: S, subscriber: Subscriber[F, K, V, S])(use: ConsumerControl[F, K, V] => Stream[F, O]): Stream[F, O] = Stream.bracket(createKafkaConsumer[F, K, V](settings))({ consumer =>
    Stream.eval_(subscriber.subscribe(consumer)(subscription)) ++ use(consumer)
  }, _.close)

  private[kafka] def messageStream[F[_]: Async, K, V, S <: Subscription](settings: ConsumerSettings[K, V], subscription: S, subscriber: Subscriber[F, K, V, S], builder: MessageBuilder[F, K, V]): Stream[F, builder.Message] = makeConsumerRecordStream(settings, subscription, subscriber) { (consumer: ConsumerControl[F, K, V]) =>
    consumer.pollStream
      .through(builder.build(consumer)(_))
  }

  def plainStream[F[_]: Async, K, V](settings: ConsumerSettings[K, V], Subscription: Subscription): Stream[F, ConsumerRecord[K, V]] = messageStream[F, K, V, Subscription](settings, Subscription, new SimpleSubscriber, new PlainMessageBuilder[F, K, V])

  def commitableMessageStream[F[_]: Async, K, V, S <: Subscription](settings: ConsumerSettings[K, V], Subscription: S): Stream[F, CommitableMessage[F, ConsumerRecord[K, V]]] = messageStream[F, K, V, S](settings, Subscription, new SimpleSubscriber[F, K, V], new CommitableMessageBuilder(settings))
}

trait ConsumerControl[F[_], K, V] {
  def close: F[Unit]
  def assignment: F[Set[TopicPartition]]
  def subscription: F[Set[String]]
  def assign(assignment: ManualSubscription): F[Unit]
  def subscribe(subscription: AutoSubscription, listener: ConsumerRebalanceListener): F[Unit]
  def poll: F[Chunk[ConsumerRecord[K, V]]]
  def pollStream: Stream[F, ConsumerRecord[K, V]]
  def wakeup: F[Unit]
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

