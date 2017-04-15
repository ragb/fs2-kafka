package co.enear.fs2.kafka

import fs2._
import fs2.util.Async

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.Serializer
import util.Properties

object Producer {
  private[kafka] def newKafkaProducer[F[_], K, V](settings: ProducerSettings)(implicit keySerializer: Serializer[K], valueSerializer: Serializer[V], F: Async[F]): F[ProducerControl[F, K, V]] = F.delay {
    new ProducerControlImpl[F, K, V](
      new KafkaProducer(Properties.fromMap(settings.properties), keySerializer, valueSerializer)
    )
  }

  def kafkaProducer[F[_]: Async, K: Serializer, V: Serializer, O](settings: ProducerSettings)(
    use: ProducerControl[F, K, V] => Stream[F, O]
  ): Stream[F, O] = Stream.bracket[F, ProducerControl[F, K, V], O](newKafkaProducer[F, K, V](settings))(use, _.close)

}

trait ProducerControl[F[_], K, V] {
  def close: F[Unit]
  def sendAsync(record: ProducerRecord[K, V]): F[Unit]
  def sendSync[P](producerMessage: ProducerMessage[K, V, P]): F[ProducerMetadata[P]]

  final def send[P]: Pipe[F, ProducerMessage[K, V, P], ProducerMetadata[P]] = _.evalMap(sendSync _)
  final def sendSink: Sink[F, ProducerRecord[K, V]] = _.evalMap(sendAsync _)
}

private[kafka] class ProducerControlImpl[F[_], K, V](producer: Producer[K, V])(implicit F: Async[F]) extends ProducerControl[F, K, V] {
  override def close = F.delay {
    producer.close()
  }

  override def sendAsync(producerRecord: ProducerRecord[K, V]) = F.delay {
    producer.send(producerRecord)
    ()
  }

  override def sendSync[P](producerMessage: ProducerMessage[K, V, P]) = F.async[ProducerMetadata[P]] { register =>
    F.delay {
      producer.send(producerMessage.producerRecord, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception) = {
          register(Option(metadata).map(m => Right(ProducerMetadata(m, producerMessage.passthrough)))
            .getOrElse(Left(exception)))
        }
      })
      ()
    }
  }
}
