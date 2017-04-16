package co.enear.fs2.kafka

import scala.concurrent.duration._
import testing.MockConsumer
import org.specs2._
import org.specs2.concurrent.ExecutionEnv

import fs2._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import DefaultSerialization._

class ConsumerSpec(implicit executionEnv: ExecutionEnv) extends mutable.Specification {
  implicit val strategy = Strategy.fromFixedDaemonPool(16, "worker")
  val testTopic = "test"
  val manualSubscription = Subscriptions.assignment(Set(new TopicPartition(testTopic, 0)))
  val settings = ConsumerSettings[String, String](50 millis).withGroupId("test")

  val messages = (1 to 10).toSeq
  val records = Stream.emits[Pure, Int](messages).
    map(offset => new ConsumerRecord[String, String](testTopic, 0, offset.toLong, "key", offset.toString))
  "consumer" should {
    "Consume messages" in {
      val messages = (1 to 10).toSeq
      val records = Stream.emits[Pure, Int](messages).
        map(offset => new ConsumerRecord[String, String](testTopic, 0, offset.toLong, "key", offset.toString))
      val consumerStream = MockConsumer[Task, String, String](settings, manualSubscription, records).plainStream(manualSubscription)
        .take(messages.size.toLong)

      consumerStream.runLog
        .unsafeRun.size must beEqualTo(messages.size)

    }

    "commit messages" in {
      val consumerStream = MockConsumer[Task, String, String](settings, manualSubscription, records).commitableMessageStream(manualSubscription)
        .take(messages.size.toLong)
        .evalMap(_.commitableOffset.commit)
      consumerStream.runLog
        .unsafeRun.size must beEqualTo(messages.size)
    }
  }
}
