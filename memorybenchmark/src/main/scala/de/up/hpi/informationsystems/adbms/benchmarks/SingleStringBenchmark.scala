package de.up.hpi.informationsystems.adbms.benchmarks

import java.io.File

import scala.io.Source

object SingleStringBenchmark extends App {
  val dataDir = "/Users/frederic/repos/uni/actordb/memorybenchmark/data/"

  // == Dependency Setup ==
  def recursiveListFiles(d: File): List[File] = {
    val these = d.listFiles()
    these.filter(_.isFile).toList ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def readStringFromFile(f: File): String = {
    val bufferedSource = Source.fromFile(f)
    val result = bufferedSource.getLines().reduceLeft(_+_)
    bufferedSource.close()
    result
  }

  // ======= Main ========
  val fileList = recursiveListFiles(new File(dataDir))

  var string = ""
  fileList.foreach(f => {
    string = string.concat(readStringFromFile(f))
  })
  // val strings = fileList.map(readStringFromFile)

  while (true) {}

  println(s"$string")
}
