package de.up.hpi.informationsystems.adbms.benchmarks.performance

object Implicits {

  implicit class StatsSupport(col: Seq[Long]) {

    def avg(): Double = {
      col.sum / col.size
    }

    def median(): Double = {
      if(col.isEmpty) return 0.0
      val sorted = col.sorted
      val size = sorted.size
      (sorted(size/2) + sorted(size - size/2))/2
    }
  }
}
