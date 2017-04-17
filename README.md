## fs2 Kafka ##


<a href='https://bintray.com/batista/maven/fs2-kafka?source=watch' alt='Get automatic notifications about new "fs2-kafka" versions'><img src='https://www.bintray.com/docs/images/bintray_badge_color.png'></a>

[fs2][fs2] wraper for [Apache Kafka][kafka]. Wrapes the Java consumer/producer Kafka APIs.

Proof of concept code.

### Features ###

* Raw `ConsumerRecord` stream API
* Sync and Async producer API.
* Commitable message support (for manual offset commits)
* Automatic and Manual subscription to topics and partitions.

### Dependencies ###

Artifacts are released to my [bintray repo][bintrayrepo]

To use this library in a SBT project:

```scala
resolvers += Resolver.bintrayRepo("batista", "maven")
libraryDependencies += "co.enear.fs2" %% "fs2-kafka" % "0.0.3"
```

[fs2]: https://github.com/functional-streams-for-scala/fs2
[kafka]: https://kafka.apache.org
[bintrayrepo]: https://bintray.com/batista/maven