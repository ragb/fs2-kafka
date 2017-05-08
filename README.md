## fs2 Kafka ##

<a href='https://bintray.com/batista/maven/fs2-kafka?source=watch'><img src='https://www.bintray.com/docs/images/bintray_badge_bw.png' alt='Get automatic notifications about new "fs2-kafka" versions'></a>


[fs2][fs2] wraper for [Apache Kafka][kafka]. Wrapes the Java consumer/producer Kafka APIs.

Proof of concept code.

### Features ###

* Raw `ConsumerRecord` stream API
* Sync and Async producer API.
* Commitable message support (for manual offset commits)
* Automatic and Manual subscription to topics and partitions.
* Partitioned streams to leverage concurrency within Kafka partitions.

### Dependencies ###

Artifacts are released to my [bintray repo][bintrayrepo]

To use this library in your SBT project:

```scala
resolvers += Resolver.bintrayRepo("batista", "maven")
libraryDependencies += "co.enear.fs2" %% "fs2-kafka" % "0.0.8"
```

### Example ###

```scala
import scala.concurrent.duration._
import fs2._
import co.enear.fs2.kafka._
import DefaultSerialization._


val bootstrapServers = "localhost:9092"
val consumerGroup = "myConsumer"
val consumerSettings = ConsumerSettings[String, String](100 millis)
  .withBootstrapServers(bootstrapServers)
  .withGroupId(consumerGroup)

val producerSettings = ProducerSettings[String, String]()
  .withBootstrapServers(bootstrapServers)

// Consume from one topic and say hello to another
val simpleStream = Consumer[Task, String, String](consumerSettings)
  .simpleStream 
  .plainMessages(Subscription.topics("names"))
  .map("hello" + _.value)
  .map(new ProducerRecord("topic2", null, _))
  .to(Producer[Task, String, String](producerSettings).sendAsync)
```

[fs2]: https://github.com/functional-streams-for-scala/fs2
[kafka]: https://kafka.apache.org
[bintrayrepo]: https://bintray.com/batista/maven