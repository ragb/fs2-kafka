import sbt._

object Dependencies {
  lazy val fs2Core = "co.fs2" %% "fs2-core" % fs2Version
  lazy val fs2Extensions = "com.ruiandrebatista" %% "fs2-extensions" % fs2ExtensionsVersion
  lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % kafkaClientsVersion
  lazy val specs2Core = "org.specs2" %% "specs2-core" % specs2Version
  lazy val dockerItScalaSpecs2 = "com.whisk" %% "docker-testkit-specs2" % dockerItScalaVersion
  lazy val dockerItScalaSpotify = "com.whisk" %% "docker-testkit-impl-spotify" % dockerItScalaVersion
  lazy val slf4jSimple = "org.slf4j" % "slf4j-simple" % slf4jVersion
  lazy val kindProjector = compilerPlugin("org.spire-math" %% "kind-projector" % kindProjectorVersion cross CrossVersion.binary)

  val fs2ExtensionsVersion = "0.1.3"
  val specs2Version = "3.8.8"
  val fs2Version = "0.9.5"
  val kafkaClientsVersion = "0.10.2.0"
  val dockerItScalaVersion = "0.9.0"
  val slf4jVersion = "1.7.24"
  val kindProjectorVersion = "0.9.3"
}

