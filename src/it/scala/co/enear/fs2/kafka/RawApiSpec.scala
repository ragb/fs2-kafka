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
  implicit val strategy = Strategy.fromFixedDaemonPool(2, "kafka integration")
  implicit val scheduler = Scheduler.fromFixedDaemonPool(2, "scheduler")

  sequential

  val testTopic = "test"
  val bootstrapServers = s"localhost:${KafkaAdvertisedPort}"
  val producerSettings = ProducerSettings().withBootstrapServers(bootstrapServers)
  val consumerSettings = ConsumerSettings[String, String](100 millis).withBootstrapServers(bootstrapServers)
    .withGroupId("test")
    .withAutoOffsetReset("earliest")
    .withAutoCommit(true)
  val subscription = Subscription.AutoSubscription(Set(testTopic))


  "Raw API" should {
    "Doroundtrip" in {
      val producerStream = Producer.kafkaProducer[Task, String, String, Nothing](producerSettings) { producerControl =>
          Stream[Task, String]("hello")
          .map(str => ProducerMessage[String, String, Unit](new ProducerRecord(testTopic, "key", str), ()))
            .through(producerControl.send[Unit])
          .drain
        }

      val consumerStream = time.sleep_[Task](2 seconds) ++  Consumer.plainStream[Task, String, String](consumerSettings, subscription)
        .map(_.value)
        .take(1)


      (producerStream ++ consumerStream)
        .runLast.unsafeRunAsyncFuture must beEqualTo(Some("hello")).await(0, 60 seconds)
    }

    "Consume produced messages" in {
      val count = 200000
      val producerStream = Producer.kafkaProducer[Task, String, String, Unit](producerSettings) { producerControl =>
          Stream.range(0, count)
            .covary[Task]
            .map(_.toString)
            .map(str => new ProducerRecord(testTopic, "test", str))
            .to(producerControl.sendSink())

        }

      val consumerStream = Consumer.plainStream[Task, String, String](consumerSettings, subscription)
        .map(_.value)
        .take(count.toLong)
      .map(_ => 1)

      val sum = for {
        _ <- producerStream.run
sum <- consumerStream.runFold[Int](0)(_ + _)
      } yield sum
      sum.unsafeRunAsyncFuture must beEqualTo(count).await(0, 60 seconds)
    }

    "Commit messages" in {
            val producerStream = Producer.kafkaProducer[Task, String, String, Nothing](producerSettings) { producerControl =>
          Stream[Task, String]("hello")
          .map(str => ProducerMessage[String, String, Unit](new ProducerRecord(testTopic, "key", str), ()))
            .through(producerControl.send[Unit])
          .drain
        }

      val consumerStream = Consumer.commitableMessageStream[Task, String, String](consumerSettings.withAutoCommit(false), subscription)
      .take(1)
        .evalMap(_.commitableOffset.commit)
    
    (producerStream ++ consumerStream)
      .run
      .unsafeRunAsyncFuture must beEqualTo(()).await(0, 10 seconds)
  }

  }
}
