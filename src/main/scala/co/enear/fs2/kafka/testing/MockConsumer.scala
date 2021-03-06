package co.enear.fs2.kafka
package testing

import internal._
import fs2._
import fs2.util.Async
import fs2.util.syntax._
import org.apache.kafka.clients.consumer.{ ConsumerRecord, MockConsumer => KafkaMockConsumer, OffsetResetStrategy }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.KafkaException

object MockConsumer {
  def apply[F[+_], K, V](settings: ConsumerSettings[K, V], recordsStream: MockConsumerControl[F, K, V] => Stream[F, ConsumerRecord[K, V]])(implicit F: Async[F]): Consumer[F, K, V] = {
    val rawConsumer = new KafkaMockConsumer[K, V](OffsetResetStrategy.EARLIEST)
    val addRecordsStream = (c: MockConsumerControl[F, K, V]) =>
      recordsStream(c)
        .evalMap(c.addRecord _)

    val create = async.mutable.Queue.unbounded[F, F[Unit]].map { queue =>
      new MockConsumerControlImpl(rawConsumer, settings, queue)
    }
    new Consumer[F, K, V] {
      override val createConsumer = for {
        consumer <- create
        _ <- addRecordsStream(consumer).run
      } yield consumer

    }

  }
}

trait MockConsumerControl[F[_], K, V] extends ConsumerControl[F, K, V] {
  def setException(exception: KafkaException): F[Unit]
  def updateBeginningOffsets(offsets: Map[TopicPartition, Long]): F[Unit]
  def updateEndOffsets(offsets: Map[TopicPartition, Long]): F[Unit]
  def addRecord(record: ConsumerRecord[K, V]): F[Unit]
}

