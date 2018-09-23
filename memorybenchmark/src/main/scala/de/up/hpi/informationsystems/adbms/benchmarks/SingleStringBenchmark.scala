package de.up.hpi.informationsystems.adbms.benchmarks

import java.io.File

import scala.io.Source

object SingleStringBenchmark extends App {
  val dataDir = "/data/relationtest/data-10000"

  // == Dependency Setup ==
  case class StringHolder(s: String)

  def recursiveListFiles(d: File): List[File] = {
    val these = d.listFiles()
    these.filter(_.isFile).toList ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }
  def readStringFromFileUsingBuilder(f: File, sb: StringBuilder): String = {
    val source = Source.fromFile(f)
    source.foreach(sb.append)
    sb.toString()
  }

  // ======= Main ========
  val dataURL = getClass.getResource(dataDir)
  val fileList = recursiveListFiles(new File(dataURL.getPath))

  val sb = new StringBuilder
  fileList.foreach(f => readStringFromFileUsingBuilder(f, sb))
  val string: StringHolder = StringHolder(sb.toString)

  println("loaded")
  while (true) {
    Thread.sleep(500)
  }

  // pretend usage to prevent GC
  println(s"$string")
}
