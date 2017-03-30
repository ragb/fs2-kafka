
import Dependencies._

lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(
    inThisBuild(List(
      organization := "co.enear.fs2",
      scalaVersion := "2.11.8",
      licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
    )),
    name := "fs2-kafka",
    libraryDependencies ++= Seq(
      fs2Core,
      kafkaClients,
      specs2Core % "it,test",
      slf4jSimple % "it,test",
      dockerItScalaSpecs2 % "it",
      dockerItScalaSpotify % "it",
      kindProjector
    ),
      scalacOptions ++= Seq(
  "-deprecation",           
  "-encoding", "UTF-8",       // yes, this is 2 args
  "-feature",                
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xfatal-warnings",       
  "-Xlint",
  "-Yno-adapted-args",       
  "-Ywarn-dead-code",        // N.B. doesn't work well with the ??? hole
  "-Ywarn-numeric-widen",   
  "-Ywarn-value-discard",
  "-Xfuture",
  "-Ywarn-unused-import"     // 2.11 only
      )
  )
.settings(Defaults.itSettings:_*)
