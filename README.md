## fs2 Kafka ##

[fs2][fs2] wraper for [Apache Kafka][kafka]. Wrapes the Java consumer/producer Kafka APIs.

Proof of concept code.

### Features ###

* Raw `ConsumerRecord` stream API
* Sync and Async producer API.
* Commitable message support (for manual offset commits)
* Automatic and Manual subscription to topics and partitions.

[fs2]: https://github.com/functional-streams-for-scala/fs2
[kafka]: https://kafka.apache.org
