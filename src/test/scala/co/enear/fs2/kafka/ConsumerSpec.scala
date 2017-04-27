package co.enear.fs2.kafka

import scala.concurrent.duration._
import testing.{ MockConsumer, MockConsumerControl }
import org.specs2._

import fs2._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.KafkaException

import DefaultSerialization._

class ConsumerSpec extends mutable.Specification {

  implicit val strategy = Strategy.fromCachedDaemonPool("fs2")
  val testTopic = "test"
  val manualSubscription = Subscriptions.assignment(Set(new TopicPartition(testTopic, 0)))
  val settings = ConsumerSettings[String, String](50 millis).withGroupId("test")

  val messages = (1 to 10).toSeq
  val recordsStream = (c: MockConsumerControl[Task, String, String]) =>
    Stream.eval_(c.assign(manualSubscription)) ++
      Stream.eval_(c.updateBeginningOffsets(Map(new TopicPartition(testTopic, 0) -> 0))) ++
      Stream.emits[Pure, Int](messages).
      map(offset => new ConsumerRecord[String, String](testTopic, 0, offset.toLong, "key", offset.toString))

  "consumer" should {
    "Consume messages" in {
      val consumerStream = MockConsumer[Task, String, String](settings, recordsStream).simpleStream.plainMessages(manualSubscription)
        .take(messages.size.toLong)

      consumerStream.runLog
        .unsafeRun.size must beEqualTo(messages.size)

    }

    "Process kafka exceptions" in {
      val recordsStream = (c: MockConsumerControl[Task, String, String]) =>
        Stream.eval_ {
          c.setException(new KafkaException("Error"))
        }

      val consumerStream = MockConsumer[Task, String, String](settings, recordsStream).simpleStream.plainMessages(manualSubscription)

      consumerStream.runLog.unsafeRun must throwAn[KafkaException]
    }

    "commit messages" in {
      val consumerStream = MockConsumer[Task, String, String](settings, recordsStream).simpleStream.commitableMessages(manualSubscription)
        .take(messages.size.toLong)
        .evalMap(_.commitableOffset.commit)
      consumerStream.runLog
        .unsafeRun.size must beEqualTo(messages.size)
    }

    "Process concurrent streams from different partitions" in {
      val numberOfPartitions = 10
      val partitionAndMessages = for {
        partition <- 0 to (numberOfPartitions - 1)
        offset <- 0 to 9999
      } yield (partition, offset)
      val partitions = (0 to (numberOfPartitions - 1)).map(new TopicPartition(testTopic, _)).toSet
      val assignment = Subscriptions.assignmentWithOffsets(partitions.map(_ -> 0l).toMap)
      val partitionRecordsStream = (c: MockConsumerControl[Task, String, String]) =>
        Stream.eval(c.assign(assignment)) >>
          Stream.eval(c.updateBeginningOffsets(partitions.map((_, 0l)).toMap)) >>
          Stream.emits(partitionAndMessages)
          .map { case (partition, offset) => new ConsumerRecord[String, String](testTopic, partition, offset.toLong, "key", offset.toString) }

      val consumerStreams = MockConsumer[Task, String, String](settings, partitionRecordsStream).partitionedStreams.plainMessages(assignment)
        .map(_._2)
      fs2.concurrent.join(numberOfPartitions)(consumerStreams)
        .take(partitionAndMessages.size.toLong)
        .runLog
        .unsafeRun.size must beEqualTo(partitionAndMessages.size)
    }
  }
}
