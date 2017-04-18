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

  private[kafka] def makeConsumerRecordStream[O, S <: Subscription](subscription: S, subscriber: Subscriber[F, K, V, S])(use: ConsumerControl[F, K, V] => Stream[F, O])(implicit F: Async[F]): Stream[F, O] = Stream.bracket(createConsumer)({ consumer =>
    Stream.eval_(subscriber.subscribe(consumer)(subscription)) ++ use(consumer)
  }, _.close)

  private[kafka] def messageStream[S <: Subscription](subscription: S, subscriber: Subscriber[F, K, V, S], builder: MessageBuilder[F, K, V])(implicit F: Async[F]): Stream[F, builder.Message] = makeConsumerRecordStream(subscription, subscriber) { (consumer: ConsumerControl[F, K, V]) =>
    consumer.pollStream
      .flatMap(consumerRecords => Stream.emits(consumerRecords.iterator.asScala.toSeq))
      .through(builder.build(consumer)(_))
  }

  def plainStream(subscription: Subscription)(implicit F: Async[F]): Stream[F, ConsumerRecord[K, V]] = messageStream[Subscription](subscription, new SimpleSubscriber, new PlainMessageBuilder[F, K, V])

  def commitableMessageStream[S <: Subscription](subscription: S)(implicit F: Async[F]): Stream[F, CommitableMessage[F, ConsumerRecord[K, V]]] = messageStream[S](subscription, new SimpleSubscriber[F, K, V], new CommitableMessageBuilder)

  private[kafka] def makeInnerPartitionedStreams(
    subscriptionEventsQueue: async.mutable.Queue[F, SubscriptionEvent],
    openedStreams: Async.Ref[F, Map[TopicPartition, async.mutable.Queue[F, Option[Chunk[ConsumerRecord[K, V]]]]]],
    killSignal: async.mutable.Signal[F, Boolean]
  )(
    consumer: ConsumerControl[F, K, V]
  )(implicit F: Async[F]): Stream[F, (TopicPartition, Stream[F, ConsumerRecord[K, V]])] = {
    subscriptionEventsQueue.dequeue
      .flatMap { event =>
        event match {
          case SubscriptionEvent.PartitionsAssigned(partitions) =>
            // Create one queue for each opened partition
            val nowOpen = partitions.toSeq.map { partition =>
              async.mutable.Queue.synchronousNoneTerminated[F, Chunk[ConsumerRecord[K, V]]].map { queue =>
                (partition, queue)
              }
            }
              .sequence
            Stream.eval(nowOpen)
              .flatMap { open =>
                Stream.eval_(openedStreams.modify(_ ++ open.toMap)) ++
                  Stream.emits {
                    open.map {
                      case (partition, queue) =>
                        (partition,
                          Stream.eval_(consumer.resume(Set(partition))) ++ queue
                          .dequeue
                          .unNoneTerminate
                          .flatMap(Stream.chunk _)
                          .interruptWhen(killSignal))
                    }
                  }
              }

          case SubscriptionEvent.PartitionsRevoked(revoked) =>
            Stream.eval_ {
              openedStreams.get flatMap { open =>
                revoked.toSeq.map { open.apply _ }
                  .map { queue =>
                    queue.enqueue1(None)
                  }
                  .sequence >> openedStreams.modify(_ -- revoked)
              }
            }
        }

      }
  }

  private[kafka] def partitionedStream(subscription: AutoSubscription, builder: MessageBuilder[F, K, V])(implicit F: Async[F]): Stream[F, (TopicPartition, Stream[F, builder.Message])] = for {
    openedStreams <- Stream.eval(F.refOf[Map[TopicPartition, async.mutable.Queue[F, Option[Chunk[ConsumerRecord[K, V]]]]]](Map.empty))
    subscriptionEventsQueue <- Stream.eval(async.mutable.Queue.unbounded[F, SubscriptionEvent])
    killSignal <- Stream.eval(async.mutable.Signal[F, Boolean](false))
    subscriber = new AsyncSubscriber[F, K, V](subscriptionEventsQueue)
    consumerStream = makeConsumerRecordStream(subscription, subscriber) { consumer =>
      val partitionsStream = makeInnerPartitionedStreams(subscriptionEventsQueue, openedStreams, killSignal)(consumer)
        .map { case (partition, stream) => (partition, stream.through(builder.build(consumer)(_))) }

      val pollStream = Stream.eval_(subscriber.subscribe(consumer)(subscription)) ++
        consumer.pollStream
        .evalMap { records =>
          openedStreams.get.flatMap { open =>
            records.partitions.asScala.toSeq.map { partition =>
              val partitionRecords = records.records(partition).asScala
              open(partition).enqueue1(Some(Chunk.seq(partitionRecords)))
            }
              .sequence
          }
        }
      (pollStream mergeDrainL partitionsStream)
        .onFinalize(killSignal.set(true))
    }
    partitionAndStream <- consumerStream
  } yield partitionAndStream
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
