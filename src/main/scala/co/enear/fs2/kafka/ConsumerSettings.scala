package co.enear.fs2.kafka

import scala.concurrent.duration.FiniteDuration
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer

case class ConsumerSettings[K, V](pollInterval: FiniteDuration, properties: Map[String, String] = Map.empty)(implicit val keyDeserializer: Deserializer[K], val valueDeserializer: Deserializer[V]) {
  def withBootstrapServers(bootstrapServers: String) = withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  def withAutoCommit(autoCommit: Boolean) = withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit.toString)
  def withGroupId(groupId: String) = withProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  def withPollInterval(interval: FiniteDuration) = copy[K, V](pollInterval = interval)
  def withMaxPollRecords(maxPollRecords: Long) = withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
  def withAutoOffsetReset(reset: String) = withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, reset)

  def withProperty(key: String, value: String) = copy[K, V](properties = properties.updated(key, value))

}

sealed trait Subscription

object Subscription {
  type ConsumerRebalanceListenerFunction = Consumer[_, _] => Iterable[TopicPartition] => Unit

  case class AutoSubscription(topics: Set[String], onAssignedPartitions: Option[ConsumerRebalanceListenerFunction] = None, onRevokedPartitions: Option[ConsumerRebalanceListenerFunction] = None) extends Subscription

  case class ManualAssignment(assignments: Map[String, Int]) extends Subscription
}
