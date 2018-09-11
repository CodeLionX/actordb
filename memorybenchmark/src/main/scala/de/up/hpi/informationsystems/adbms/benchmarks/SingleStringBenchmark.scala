package de.up.hpi.informationsystems.adbms.benchmarks

import java.io.File

import scala.io.Source

object SingleStringBenchmark extends App {
  val dataDir = "/data/relationtest/data-10000000"

  // == Dependency Setup ==
  class StringHolder(val s: String){
    def concat(s2: String): StringHolder = new StringHolder(s.concat(s2))
    def concat(sh2: StringHolder): StringHolder = new StringHolder(s.concat(sh2.s))
  }

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

  def readStringFromFileUsingBuilder(f: File): String = {
    val source = Source.fromFile(f)
    val sb = new StringBuilder
    source.foreach(sb.append)
    sb.toString()
  }

  // ======= Main ========
  val dataURL = getClass.getResource(dataDir)
  val fileList = recursiveListFiles(new File(dataURL.getPath))

  var string: StringHolder = new StringHolder("")
  fileList.foreach(f => {
    string = string.concat(readStringFromFileUsingBuilder(f))
  })
  // val strings = fileList.map(readStringFromFile)

  println("loaded")
  while (true) {
    Thread.sleep(500)
  }

  println(s"$string")
}
