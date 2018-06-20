name := "actordb"

logBuffered in Test := false

lazy val adbms = (project in file("adbms"))
  .settings(
    Common.commonSettings,
    // I don't know why they are not accepted at the commonSettings key
    organization := Common.organization,
    scalaVersion := Common.scalaVersion,
    libraryDependencies ++= Dependencies.akkaActorDependencies,
    libraryDependencies ++= Dependencies.uniVocityParsers
  )

lazy val sampleapp = (project in file("sampleapp"))
  .settings(
    Common.commonSettings,
    // I don't know why they are not accepted at the commonSettings key
    organization := Common.organization,
    scalaVersion := Common.scalaVersion,
    libraryDependencies ++= Dependencies.akkaActorDependencies
  )
  .dependsOn(adbms)

lazy val fouleggs = (project in file("fouleggs"))
  .settings(
    Common.commonSettings,
    organization := Common.organization,
    scalaVersion := Common.scalaVersion,
    libraryDependencies ++= Dependencies.akkaActorDependencies
  )
  .dependsOn(adbms)

lazy val root = (project in file("."))
  .aggregate(adbms, sampleapp, fouleggs)
  .settings(
    version := Common.frameworkVersion,
    scalaVersion := Common.scalaVersion
  )

lazy val memorybenchmark = (project in file("memorybenchmark"))
  .settings(
    Common.commonSettings,
    // I don't know why they are not accepted at the commonSettings key
    organization := Common.organization,
    scalaVersion := Common.scalaVersion,
    libraryDependencies ++= Dependencies.akkaActorDependencies
  )
  .dependsOn(adbms, sampleapp)
