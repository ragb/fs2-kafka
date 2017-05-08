package co.enear.fs2.kafka

import util.Properties
import internal._
import fs2._
import fs2.util.Async
import com.ruiandrebatista.fs2.extensions.syntax._

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.Serializer

trait Producer[F[_], K, V] {
  private[kafka] def createProducer: F[ProducerControl[F, K, V]]

  private[kafka] def withProducer[I, O](f: ProducerControl[F, K, V] => Pipe[F, I, O]): Pipe[F, I, O] = { in: Stream[F, I] =>
    Stream.bracket(createProducer)({ producer =>
      f(producer)(in)
    }, _.close)
  }
  def sendUsing[I, O](
    f: ProducerControl[F, K, V] => I => F[O]
  )(implicit F: Async[F]): Pipe[F, I, O] = withProducer[I, O] { producer => in =>
    in.mapAsync(f(producer))(producer.parallelism)
  }

  final def send[P](implicit F: Async[F]): Pipe[F, ProducerMessage[K, V, P], ProducerMetadata[P]] = sendUsing(_.sendSync[P])

  final def sendAsync: Sink[F, ProducerRecord[K, V]] = withProducer { producer => in =>
    in.evalMap(producer.sendAsync _)
  }

  final def sendCommitable[P <: Commitable[F]](implicit F: Async[F]): Sink[F, ProducerMessage[K, V, P]] = _.through(send)
    .evalMap(_.passThrough.commit)
    .drain
}

object Producer {
  private[kafka] def newKafkaProducer[F[_], K, V](settings: ProducerSettings[K, V])(implicit F: Async[F]): F[ProducerControl[F, K, V]] = F.delay {
    new ProducerControlImpl[F, K, V](
      new KafkaProducer(Properties.fromMap(settings.properties), settings.keySerializer, settings.valueSerializer),
      settings
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
  def parallelism: Int
}
