import sbt._

object Dependencies {
  val akkaVersion = "2.5.12"

  val akkaTestDependencies = Seq(
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
  )

  val akkaActorDependencies =
    akkaTestDependencies ++ Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion
    )

  val uniVocityParsers = Seq(
    "com.univocity" % "univocity-parsers" % "2.6.3"
  )
}