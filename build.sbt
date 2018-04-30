name := "actordb"

logBuffered in Test := false

lazy val adbms = (project in file("adbms"))
  .settings(
    Common.commonSettings,
    // I don't know why they are not accepted at the commonSettings key
    organization := Common.organization,
    scalaVersion := Common.scalaVersion,
    libraryDependencies ++= Dependencies.akkaActorDependencies
  )

lazy val root = (project in file("."))
  .aggregate(adbms)
  .settings(
    version := Common.frameworkVersion,
    scalaVersion := Common.scalaVersion
  )