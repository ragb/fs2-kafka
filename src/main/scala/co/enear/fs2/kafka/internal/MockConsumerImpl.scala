package co.enear.fs2.kafka
package internal

import testing._
import scala.collection.JavaConverters._
import fs2._
import fs2.util.Async

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{ TopicPartition, KafkaException }

private[kafka] class MockConsumerControlImpl[F[_], K, V](
    override val rawConsumer: MockConsumer[K, V],
    settings: ConsumerSettings[K, V],
    pollingThreadTasksQueue: async.mutable.Queue[F, F[Unit]]
)(implicit F: Async[F]) extends ConsumerControlImpl[F, K, V](rawConsumer, settings, pollingThreadTasksQueue) with MockConsumerControl[F, K, V] {

  override def setException(exception: KafkaException) = pollingThreadTasksQueue.enqueue1 {
    F.delay {
      rawConsumer.setException(exception)

    }
  }

  override def updateEndOffsets(offsets: Map[TopicPartition, Long]) = F.delay {
    rawConsumer.updateEndOffsets(offsets.mapValues(new java.lang.Long(_)).asJava)
  }

  override def updateBeginningOffsets(offsets: Map[TopicPartition, Long]) = F.delay {
    rawConsumer.updateBeginningOffsets(offsets.mapValues(new java.lang.Long(_)).asJava)
  }

  override def addRecord(record: ConsumerRecord[K, V]) = F.delay {
    rawConsumer.addRecord(record)
  }
}
