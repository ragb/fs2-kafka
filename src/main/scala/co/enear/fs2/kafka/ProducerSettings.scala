package co.enear.fs2.kafka

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serializer

case class ProducerSettings[K, V](properties: Map[String, String] = Map.empty, parallelism: Int = 100)(implicit
  val keySerializer: Serializer[K],
    val valueSerializer: Serializer[V]) {

  def withBootstrapServers(bootstrapServers: String) = withProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

  def withAcks(acks: String) = withProperty(ProducerConfig.ACKS_CONFIG, acks)
  def withRetries(retries: Int) = withProperty(ProducerConfig.RETRIES_CONFIG, retries.toString)

  def withProperty(key: String, value: String) = ProducerSettings[K, V](properties.updated(key, value))

  def withParallelism(parallelism: Int) = copy(parallelism = parallelism)(keySerializer, valueSerializer)

}
