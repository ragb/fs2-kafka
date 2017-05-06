package co.enear.fs2.kafka

import util.Properties
import internal._
import fs2._
import fs2.util.Async

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.Serializer

trait Producer[F[_], K, V] {
  private[kafka] def createProducer: F[ProducerControl[F, K, V]]

  def sendUsing[I, O](f: ProducerControl[F, K, V] => I => F[O]): Pipe[F, I, O] = { in =>
    Stream.bracket(createProducer)({ producer =>
      in.evalMap(f(producer))
    }, _.close)
  }

  final def send[P]: Pipe[F, ProducerMessage[K, V, P], ProducerMetadata[P]] = sendUsing(_.sendSync[P])

  final def sendAsync: Sink[F, ProducerRecord[K, V]] = sendUsing(_.sendAsync)

  final def sendCommitable[P <: Commitable[F]]: Sink[F, ProducerMessage[K, V, P]] = _.through(send)
    .evalMap(_.passThrough.commit)
    .drain
}

object Producer {
  private[kafka] def newKafkaProducer[F[_], K, V](settings: ProducerSettings[K, V])(implicit F: Async[F]): F[ProducerControl[F, K, V]] = F.delay {
    new ProducerControlImpl[F, K, V](
      new KafkaProducer(Properties.fromMap(settings.properties), settings.keySerializer, settings.valueSerializer)
    )
  }

  def apply[F[_]: Async, K: Serializer, V: Serializer](settings: ProducerSettings[K, V]) = new Producer[F, K, V] {
    override def createProducer = newKafkaProducer[F, K, V](settings)
  }

}

trait ProducerControl[F[_], K, V] {
  def close: F[Unit]
  def sendAsync(record: ProducerRecord[K, V]): F[Unit]
  def sendSync[P](producerMessage: ProducerMessage[K, V, P]): F[ProducerMetadata[P]]

}
