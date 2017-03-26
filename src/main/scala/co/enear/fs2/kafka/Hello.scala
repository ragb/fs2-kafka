package co.enear.fs2

object Hello extends Greeting with App {
  println(greeting)
}

trait Greeting {
  lazy val greeting: String = "hello fs2 Kafka"
}
