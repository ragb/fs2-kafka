package co.enear.fs2.kafka

import org.apache.kafka.clients.producer._

case class ProducerMessage[K, V, Passthrough](producerRecord: ProducerRecord[K, V], passthrough: Passthrough)

case class ProducerMetadata[PassThrough](metadata: RecordMetadata, passThrough: PassThrough)

