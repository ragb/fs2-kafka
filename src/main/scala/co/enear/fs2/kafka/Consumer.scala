package co.enear.fs2.kafka

import fs2._
import fs2.util.{ Async, Functor }
import fs2.util.syntax._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer._
import util.Properties
import internal._

trait Consumer[F[_], K, V] {

  private[kafka] def createConsumer: F[ConsumerControl[F, K, V]]

  private[kafka] def makeConsumerRecordStream[O, S <: Subscription](subscription: S, subscriber: Subscriber[F, K, V, S])(use: ConsumerControl[F, K, V] => Stream[F, O])(implicit F: Async[F]): Stream[F, O] = Stream.bracket(createConsumer)({ consumer =>
    Stream.eval_(subscriber.subscribe(consumer)(subscription)) ++ use(consumer)
  }, _.close)

  private[kafka] def messageStream[S <: Subscription](subscription: S, subscriber: Subscriber[F, K, V, S], builder: MessageBuilder[F, K, V])(implicit F: Async[F]): Stream[F, builder.Message] = makeConsumerRecordStream(subscription, subscriber) { (consumer: ConsumerControl[F, K, V]) =>
    consumer.pollStream
      .through(builder.build(consumer)(_))
  }

  def plainStream(subscription: Subscription)(implicit F: Async[F]): Stream[F, ConsumerRecord[K, V]] = messageStream[Subscription](subscription, new SimpleSubscriber, new PlainMessageBuilder[F, K, V])

  def commitableMessageStream[S <: Subscription](subscription: S)(implicit F: Async[F]): Stream[F, CommitableMessage[F, ConsumerRecord[K, V]]] = messageStream[S](subscription, new SimpleSubscriber[F, K, V], new CommitableMessageBuilder)
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

  def createMockConsumer[F[+_], K, V](settings: ConsumerSettings[K, V], records: Stream[F, ConsumerRecord[K, V]])(implicit F: Async[F]): Consumer[F, K, V] = {
    val rawConsumer = new MockConsumer[K, V](OffsetResetStrategy.EARLIEST)
    val createAndRunStream = records
      .to {
        _.evalMap {
          record =>
            F.delay {
              rawConsumer.addRecord(record)
            }
        }
      }
      .run
      .start >> async.mutable.Queue.unbounded[F, F[Unit]].map { queue =>
        new ConsumerControlImpl(rawConsumer, settings, queue)
      }
    new Consumer[F, K, V] {
      override val createConsumer = createAndRunStream
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
