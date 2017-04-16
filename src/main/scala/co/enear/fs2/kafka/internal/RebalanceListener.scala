package co.enear.fs2.kafka
package internal

import scala.collection.JavaConverters._

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer._

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
