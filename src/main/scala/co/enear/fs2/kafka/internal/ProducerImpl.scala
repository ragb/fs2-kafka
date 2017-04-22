package co.enear.fs2.kafka
package internal

import fs2.util.Async
import org.apache.kafka.clients.producer._

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
