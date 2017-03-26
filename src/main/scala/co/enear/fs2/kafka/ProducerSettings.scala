package co.enear.fs2.kafka

import org.apache.kafka.clients.producer.ProducerConfig

case class ProducerSettings(properties: Map[String, String] = Map.empty) {
  def withBootstrapServers(bootstrapServers: String) = withProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

  def withAcks(acks: String) = withProperty(ProducerConfig.ACKS_CONFIG, acks)
  def withRetries(retries: Int) = withProperty(ProducerConfig.RETRIES_CONFIG, retries.toString)

  def withProperty(key: String, value: String) = copy(properties = properties.updated(key, value))

}
