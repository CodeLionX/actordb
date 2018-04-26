import sbt._
import Keys._

object Common {
  val organization = "de.up.hpi.informationsystems"
  val frameworkVersion = "0.0.1"
  val scalaVersion = "2.12.5"

  val commonSettings = Seq(
    //organization := organization,
    version := frameworkVersion,
    //scalaVersion := Common.scalaVersion,
    javacOptions ++= Seq(),
    scalacOptions ++= Seq("-unchecked")
  )
}