package co.enear.fs2.kafka

import scala.collection.JavaConverters._
import fs2._
import fs2.util.{ Async, Functor }
import fs2.util.syntax._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
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

  private[kafka] def makeConsumerRecordStream[F[_]: Async, K, V, O, S <: Subscription](settings: ConsumerSettings[K, V], subscription: S, subscriber: Subscriber[F, K, V, S])(use: ConsumerControl[F, K, V] => Stream[F, O]): Stream[F, O] = Stream.bracket(createKafkaConsumer[F, K, V](settings))({ consumer =>
    Stream.eval_(subscriber.subscribe(consumer)(subscription)) ++ use(consumer)
  }, _.close)

  private[kafka] def messageStream[F[_]: Async, K, V, S <: Subscription](settings: ConsumerSettings[K, V], subscription: S, subscriber: Subscriber[F, K, V, S], builder: MessageBuilder[F, K, V]): Stream[F, builder.Message] = makeConsumerRecordStream(settings, subscription, subscriber) { (consumer: ConsumerControl[F, K, V]) =>
    for {
      queue <- Stream.eval(async.mutable.Queue.synchronousNoneTerminated[F, Chunk[ConsumerRecord[K, V]]])
      done <- Stream.eval(async.mutable.Signal[F, Boolean](false))
      pollStream = Stream.eval(consumer.poll)
        .filter(_.nonEmpty)
        .map(Option.apply _)
        .to(queue.enqueue)
        .repeat
        .interruptWhen(done)
        .onFinalize(queue.enqueue1(None))
      recordsStream = queue.dequeue
        .through(pipe.unNoneTerminate)
        .flatMap(Stream.chunk _)
        .through(builder.build(consumer)(_))
      message <- (recordsStream mergeHaltBoth pollStream.drain)
        .onFinalize(consumer.wakeup >> done.set(true))
    } yield message
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

private[kafka] object RebalanceListener {
  def apply(onAssigned: Iterable[TopicPartition] => Unit, onRevoked: Iterable[TopicPartition] => Unit) = new ConsumerRebalanceListener {
    override def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]) = onAssigned(partitions.asScala)
    override def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]) = onRevoked(partitions.asScala)
  }

  def doNothing = apply(_ => (), _ => ())
  def wrapePaused(consumer: Consumer[_, _], listener: ConsumerRebalanceListener) = new ConsumerRebalanceListener {
    override def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]) = {
      consumer.pause(partitions)
      listener.onPartitionsAssigned(partitions)
    }
    override def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]) = listener.onPartitionsRevoked(partitions)
  }
}

trait Commitable[F[_]] {
  def commit: F[Commited]
}

trait CommitableOffset[F[_]] extends Commitable[F] {
  def partitionOffset: PartitionOfsset
}

trait Commiter[F[_]] {
  def commit(partitions: Map[TopicPartition, Long]): F[Commited]
}

private[kafka] class ConsumerControlImpl[F[_], K, V](consumer: Consumer[K, V], settings: ConsumerSettings[K, V])(implicit F: Async[F]) extends ConsumerControl[F, K, V] {

  override def close = F.delay {
    consumer.close()
  }

  override def assignment = F.delay {
    consumer.assignment().asScala.toSet
  }

  def subscription = F.delay {
    consumer.subscription().asScala.toSet
  }

  override def assign(assignment: ManualSubscription) = F.delay {
    assignment match {
      case Subscriptions.ManualAssignment(topicPartitions) => consumer.assign(topicPartitions.asJava)
      case Subscriptions.ManualAssignmentWithOffsets(partitionOffsets) =>
        consumer.assign(partitionOffsets.keySet.asJava)
        partitionOffsets foreach { case (topicPartition, offset) => consumer.seek(topicPartition, offset) }
    }
  }

  override def subscribe(subscription: AutoSubscription, listener: ConsumerRebalanceListener) = subscription match {
    case Subscriptions.TopicsSubscription(topics) => F.delay { consumer.subscribe(topics.asJava, listener) }
    case Subscriptions.TopicsPatternSubscription(pattern) => F.delay { consumer.subscribe(java.util.regex.Pattern.compile(pattern), listener) }
  }

  override def poll = F.delay {
    try {
      val records = consumer.poll(settings.pollInterval.toMillis).iterator.asScala.toSeq
      Chunk.seq(records)
    } catch {
      case e: WakeupException => Chunk.empty
    }
  }

  override def wakeup = F.delay {
    consumer.wakeup()
  }

  override def commiter = new Commiter[F] {
    override def commit(partitionsAndOffsets: Map[TopicPartition, Long]) = F.async[Commited] { register =>
      F.delay {
        consumer.commitAsync(partitionsAndOffsets.mapValues(offset => new OffsetAndMetadata(offset)).asJava, new OffsetCommitCallback {
          def onComplete(partitionOffsets: java.util.Map[TopicPartition, OffsetAndMetadata], exception: Exception) = {
            if (exception != null) register(Left(exception))
            else register(Right(Commited(partitionOffsets.asScala.toMap.mapValues(_.offset))))
          }
        })
      }
    }
  }
}

private[kafka] trait MessageBuilder[F[_], K, V] {
  type Message
  def build(consumer: ConsumerControl[F, K, V]): Pipe[F, ConsumerRecord[K, V], Message]
}

private[kafka] class PlainMessageBuilder[F[_], K, V] extends MessageBuilder[F, K, V] {
  type Message = ConsumerRecord[K, V]
  override def build(consumer: ConsumerControl[F, K, V]) = pipe.id
}

private[kafka] class CommitableMessageBuilder[F[_], K, V](settings: ConsumerSettings[K, V])(implicit F: Async[F]) extends MessageBuilder[F, K, V] {
  type Message = CommitableMessage[F, ConsumerRecord[K, V]]
  override def build(consumer: ConsumerControl[F, K, V]): Pipe[F, ConsumerRecord[K, V], Message] = _.map { record =>
    val offset = PartitionOfsset(GroupTopicPartition(settings.properties(ConsumerConfig.GROUP_ID_CONFIG), record.topic(), record.partition()), record.offset())
    val commitableOffset = new CommitableOffset[F] {
      override val partitionOffset = offset
      override def commit = consumer.commiter.commit(Map(new TopicPartition(partitionOffset.key.topic, partitionOffset.key.partition) -> (offset.offset + 1)))
    }
    CommitableMessage[F, ConsumerRecord[K, V]](record, commitableOffset)
  }
}

trait Subscriber[F[_], K, V, -S <: Subscription] {
  def subscribe(consumer: ConsumerControl[F, K, V])(subscription: S): F[Unit]
}

private[kafka] class SimpleSubscriber[F[_]: Async, K, V] extends Subscriber[F, K, V, Subscription] {
  override def subscribe(consumer: ConsumerControl[F, K, V])(subscription: Subscription) = subscription match {
    case s: AutoSubscription => consumer.subscribe(s, RebalanceListener.doNothing)
    case s: ManualSubscription => consumer.assign(s)
  }
}
