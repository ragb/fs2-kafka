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
      val consumerStream = MockConsumer[Task, String, String](settings, manualSubscription, records).simpleStream.plainMessages(manualSubscription)
        .take(messages.size.toLong)

      consumerStream.runLog
        .unsafeRun.size must beEqualTo(messages.size)

    }

    "commit messages" in {
      val consumerStream = MockConsumer[Task, String, String](settings, manualSubscription, records).simpleStream.commitableMessages(manualSubscription)
        .take(messages.size.toLong)
        .evalMap(_.commitableOffset.commit)
      consumerStream.runLog
        .unsafeRun.size must beEqualTo(messages.size)
    }

    "Process concurrent streams from different partitions" in {
      val numberOfPartitions = 3
      val partitionAndMessages = for {
        partition <- 0 to (numberOfPartitions - 1)
        offset <- 0 to 10
      } yield (partition, offset)
      val partitions = (0 to (numberOfPartitions - 1)).map(new TopicPartition(testTopic, _)).toSet
      val assignment = Subscriptions.assignment(partitions)
      val records = Stream.emits(partitionAndMessages)
        .map { case (partition, offset) => new ConsumerRecord[String, String](testTopic, partition, offset.toLong, "key", offset.toString) }
        .covary[Task]
      val consumerStreams = MockConsumer[Task, String, String](settings, assignment, records).partitionedStreams.plainMessages(assignment)
        .map(_._2)
        .take(numberOfPartitions.toLong)
      fs2.concurrent.join(numberOfPartitions)(consumerStreams)
        .take(partitionAndMessages.size.toLong)
        .runLog
        .unsafeRun.size must beEqualTo(partitionAndMessages.size)
    }
  }
}
