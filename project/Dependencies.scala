import sbt._

object Dependencies {
  val akkaVersion = "2.5.12"
  val akkaHttpVersion = "10.1.3"

  val akkaTestDependencies = Seq(
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
  )

  val akkaActorDependencies =
    akkaTestDependencies ++ Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion
    )

  val akkaHttpDependencies = Seq(
    "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion
  )

  val uniVocityParsers = Seq(
    "com.univocity" % "univocity-parsers" % "2.6.3"
  )
}