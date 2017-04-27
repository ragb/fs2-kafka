package co.enear.fs2.kafka


import fs2._
import fs2.time

import org.specs2._
import com.whisk.docker.specs2.DockerTestKit
import com.whisk.docker.impl.spotify.DockerKitSpotify
import org.specs2.concurrent.ExecutionEnv
import org.apache.kafka.clients.producer._

import scala.concurrent.duration._

import DefaultSerialization._

class RawApiSpec(implicit executionEnv: ExecutionEnv) extends mutable.Specification with DockerKitSpotify with DockerKafkaService with DockerTestKit{
  implicit val strategy = Strategy.fromCachedDaemonPool("fs2")
  implicit val scheduler = Scheduler.fromFixedDaemonPool(16, "scheduler")

  sequential;

  val testTopic = "test"
  val bootstrapServers = s"localhost:${KafkaAdvertisedPort}"
  val producerSettings = ProducerSettings[String, String]().withBootstrapServers(bootstrapServers)
  val consumerSettings = ConsumerSettings[String, String](100 millis).withBootstrapServers(bootstrapServers)
    .withGroupId("test")
    .withAutoOffsetReset("earliest")
    .withAutoCommit(true)
  val subscription = Subscriptions.topics(Set(testTopic))


  "Raw API" should {
    "Doroundtrip" in {
      val producerStream = Producer[Task, String, String, ProducerMetadata[Unit]](producerSettings) { producerControl =>
          Stream[Task, String]("hello")
          .map(str => ProducerMessage[String, String, Unit](new ProducerRecord(testTopic, "key", str), ()))
            .through(producerControl.send[Unit])
        }

      val consumerStream =  Consumer[Task, String, String](consumerSettings).simpleStream.plainMessages(subscription)
        .map(_.value)
        .take(1)


      (producerStream.drain ++ consumerStream)
        .runLast.unsafeRunAsyncFuture must beEqualTo(Some("hello")).await(0, 10 seconds)
    }


    "Commit messages" in {
            val producerStream = Producer[Task, String, String, Nothing](producerSettings) { producerControl =>
          Stream[Task, String]("hello")
          .map(str => ProducerMessage[String, String, Unit](new ProducerRecord(testTopic, "key", str), ()))
            .through(producerControl.send[Unit])
          .drain
        }

      val consumerStream = time.sleep_[Task](2 seconds) ++ Consumer[Task, String, String](consumerSettings.withGroupId("test3").withAutoCommit(false)).simpleStream.commitableMessages(subscription)
        .evalMap(_.commitableOffset.commit)
      .take(1)
    
    (producerStream.drain ++ consumerStream)
      .run
      .unsafeRunAsyncFuture must beEqualTo(()).await(0, 10 seconds)
  }

  }
}
