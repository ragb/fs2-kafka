package co.enear.fs2.kafka

import util.Properties
import internal._
import fs2._
import fs2.util.Async

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.Serializer

object Producer {
  private[kafka] def newKafkaProducer[F[_], K, V](settings: ProducerSettings[K, V])(implicit F: Async[F]): F[ProducerControl[F, K, V]] = F.delay {
    new ProducerControlImpl[F, K, V](
      new KafkaProducer(Properties.fromMap(settings.properties), settings.keySerializer, settings.valueSerializer)
    )
  }

  def apply[F[_]: Async, K: Serializer, V: Serializer, O](settings: ProducerSettings[K, V])(
    use: ProducerControl[F, K, V] => Stream[F, O]
  ): Stream[F, O] = Stream.bracket[F, ProducerControl[F, K, V], O](newKafkaProducer[F, K, V](settings))(use, _.close)

}

trait ProducerControl[F[_], K, V] {
  def close: F[Unit]
  def sendAsync(record: ProducerRecord[K, V]): F[Unit]
  def sendSync[P](producerMessage: ProducerMessage[K, V, P]): F[ProducerMetadata[P]]

  final def send[P]: Pipe[F, ProducerMessage[K, V, P], ProducerMetadata[P]] = _.evalMap(sendSync _)
  final def sendSink: Sink[F, ProducerRecord[K, V]] = _.evalMap(sendAsync _)
    .drain
}

