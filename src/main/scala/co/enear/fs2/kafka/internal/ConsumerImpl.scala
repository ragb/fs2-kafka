package co.enear.fs2.kafka
package internal

import scala.collection.JavaConverters._
import fs2._
import fs2.util.Async
import fs2.util.syntax._

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException

private[kafka] class ConsumerControlImpl[F[_], K, V](consumer: Consumer[K, V], settings: ConsumerSettings[K, V], pollThreadTasksQueue: async.mutable.Queue[F, F[Unit]])(implicit F: Async[F]) extends ConsumerControl[F, K, V] {

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
      Chunk.seq(consumer.poll(settings.pollInterval.toMillis).iterator.asScala.toSeq)
    } catch {
      case e: WakeupException => Chunk.empty
    }
  }

  override def pollStream = for {
    outputQueue <- Stream.eval(async.mutable.Queue.synchronousNoneTerminated[F, Chunk[ConsumerRecord[K, V]]])
    done <- Stream.eval(async.mutable.Signal(false))
    pollingThread = Stream.eval(poll)
      .flatMap { chunk =>
        Stream.eval {
          // If we have any tasks to evaluate in the same thread we poll do it here
          for {
            size <- pollThreadTasksQueue.size.get
            tasks <- if (size > 0) pollThreadTasksQueue.dequeueBatch1(size) else F.pure(Chunk.empty)
            _ <- Stream.chunk(tasks).evalMap(identity _).run
          } yield chunk
        }
      }
      .filter(_.nonEmpty)
      .repeat
      .map(Option.apply _)
      .to(outputQueue.enqueue)
      .interruptWhen(done)
      .onFinalize(outputQueue.enqueue1(None))

    outputThread = outputQueue.dequeue
      .unNoneTerminate
      .flatMap(Stream.chunk _)
    record <- (pollingThread mergeDrainL outputThread)
      .onFinalize(wakeup >> done.set(true))
  } yield record

  override def wakeup = F.delay {
    consumer.wakeup()
  }

  // This enqueues the commit task to the polling thread tasks queue
  override def commiter = new Commiter[F] {
    override def commit(partitionsAndOffsets: Map[TopicPartition, Long]) = F.async[Commited] { register =>
      pollThreadTasksQueue.enqueue1 {
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

}

