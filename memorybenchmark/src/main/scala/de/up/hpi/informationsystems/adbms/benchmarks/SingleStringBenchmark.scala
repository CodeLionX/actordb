package de.up.hpi.informationsystems.adbms.benchmarks

import java.io.File

import scala.io.Source

object SingleStringBenchmark extends App {
  val dataDir = "/data_100_mb"

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
  val dataURL = getClass.getResource(dataDir)
  val fileList = recursiveListFiles(new File(dataURL.getPath))

  var string = ""
  fileList.foreach(f => {
    string = string.concat(readStringFromFile(f))
  })
  // val strings = fileList.map(readStringFromFile)

  println("loaded")
  while (true) {
    Thread.sleep(500)
  }

  println(s"$string")
}
