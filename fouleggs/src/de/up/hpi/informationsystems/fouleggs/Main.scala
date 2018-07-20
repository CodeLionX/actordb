package de.up.hpi.informationsystems.fouleggs

import de.up.hpi.informationsystems.fouleggs.dactors.SystemInitializer

import scala.concurrent.duration._
import scala.language.postfixOps

object Main extends App {

  println("Starting system")
  SystemInitializer.initializer ! SystemInitializer.Startup(5 seconds)

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      println("Received shutdown signal from JVM")
      SystemInitializer.initializer ! SystemInitializer.Shutdown
    }
  })

}
