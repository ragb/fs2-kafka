package co.enear.fs2.kafka

import org.apache.kafka.common.serialization._

trait DefaultSerializers {
  implicit val byteArraySerializer: Serializer[Array[Byte]] = new ByteArraySerializer()
  implicit val stringSerializer: Serializer[String] = new StringSerializer()
}

trait DefaultDeserializers {
  implicit val byteArrayDeserializer: Deserializer[Array[Byte]] = new ByteArrayDeserializer()
  implicit val stringDeserializer: Deserializer[String] = new StringDeserializer()
}

trait DefaultSerialization extends DefaultSerializers with DefaultDeserializers

object DefaultSerialization extends DefaultSerialization
