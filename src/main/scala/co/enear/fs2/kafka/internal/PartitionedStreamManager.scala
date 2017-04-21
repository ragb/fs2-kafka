package co.enear.fs2.kafka
package internal

import scala.collection.JavaConverters._
import fs2._
import fs2.async.mutable.{ Queue, Signal }
import fs2.util.Async
import fs2.util.syntax._

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

private[kafka] class PartitionedStreamManager[F[_], K, V] private (
    consumer: ConsumerControl[F, K, V],
    openPartitionsQueue: Queue[F, (TopicPartition, Stream[F, ConsumerRecord[K, V]])],
    killSignal: Signal[F, Boolean],
    openPartitions: Async.Ref[F, Map[TopicPartition, Queue[F, Option[Chunk[ConsumerRecord[K, V]]]]]]
)(implicit F: Async[F]) {

  private def onAssignedPartitions(partitions: Set[TopicPartition]) = {
    // Create one queue for each opened partition
    val nowOpen = openPartitions.get.flatMap { alreadyOpen =>
      (partitions -- alreadyOpen.keys).toSeq.map { partition =>
        Queue.synchronousNoneTerminated[F, Chunk[ConsumerRecord[K, V]]].map { queue =>
          (partition, queue)
        }
      }
        .sequence
    }

    // store partitions and queues in the opened stream map
    // and emit the new created streams
    Stream.eval(nowOpen)
      .flatMap { open =>
        Stream.eval(openPartitions.modify(_ ++ open.toMap)) >>
          Stream.emits {
            open.map {
              case (partition, queue) =>
                (partition,
                  Stream.eval_(consumer.resume(Set(partition))) ++ queue
                  .dequeue
                  .unNoneTerminate
                  .flatMap(Stream.chunk _)
                  .interruptWhen(killSignal)
                  .onFinalize[F] {
                    // Should we even bother to pause the stream when it finalizes?
                    // Now we only do it if the outer stream was not interrupted 
                    for {
                      partitions <- consumer.assignment
                      interrupted <- killSignal.get
                      _ <- if ((!interrupted) && (partitions contains partition)) consumer.pause(Set(partition)) else F.pure(())
                      _ <- openPartitions.modify(_ - partition)
                    } yield ()
                  })
            }
          }
      }
      .to(openPartitionsQueue.enqueue)
      .interruptWhen(killSignal)
      .run
  }

  private def onRevokedPartitions(revoked: Set[TopicPartition]) = {
    // remove from the map and terminate the stream queues associated with revoked partitions
    openPartitions.get flatMap { open =>
      revoked.toSeq.map { open.apply _ }
        .map { queue =>
          queue.enqueue1(None)
        }
        .sequence >> openPartitions.modify(_ -- revoked)
        .map(_ => ())
    }
  }

  def partitionedStream(subscription: Subscription, builder: MessageBuilder[F, K, V]): Stream[F, (TopicPartition, Stream[F, builder.Message])] = {
    val subscriber = new AsyncSubscriber[F, K, V](onAssignedPartitions _, onRevokedPartitions _)
    val partitionsStream = openPartitionsQueue.dequeue
      .map { case (partition, stream) => (partition, stream.through(builder.build(consumer)(_))) }

    val pollStream = Stream.eval(subscriber.subscribe(consumer)(subscription)) >>
      consumer.pollStream
      .interruptWhen(killSignal)
      .flatMap { records =>
        Stream.eval {
          openPartitions.get.map { open =>
            records.partitions.asScala.toSeq.map { partition =>
              val partitionRecords = records.records(partition).asScala
              open.get(partition).map { queue => queue.enqueue1(Some(Chunk.seq(partitionRecords))) }
                .getOrElse(F.fail(new IllegalStateException(s"Received message for unknown partition $partition")))
            }
          }
        }
          .flatMap(Stream.emits _)
          .evalMap(identity _)
      }

    (pollStream.drain merge partitionsStream)
      .onFinalize(killSignal.set(true))
      .onError { case e => println(e); Stream.fail(e) }
  }

}
private[kafka] object PartitionedStreamManager {
  def apply[F[_]: Async, K, V](consumer: ConsumerControl[F, K, V]): F[PartitionedStreamManager[F, K, V]] = for {
    queue <- Queue.synchronous[F, (TopicPartition, Stream[F, ConsumerRecord[K, V]])]
    openPartitions <- Async.refOf[F, Map[TopicPartition, Queue[F, Option[Chunk[ConsumerRecord[K, V]]]]]](Map.empty)
    killSignal <- Signal[F, Boolean](false)
  } yield new PartitionedStreamManager[F, K, V](consumer, queue, killSignal, openPartitions)
}
