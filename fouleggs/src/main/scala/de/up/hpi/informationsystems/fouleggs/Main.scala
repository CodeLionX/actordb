package de.up.hpi.informationsystems.fouleggs

import akka.actor.Terminated
import de.up.hpi.informationsystems.fouleggs.dactors.SystemInitializer

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

object Main extends App {

  println("Starting system")
  SystemInitializer.initializer ! SystemInitializer.Startup(5 seconds)

  sys.addShutdownHook({
    println("Received shutdown signal from JVM")
    SystemInitializer.initializer ! SystemInitializer.Shutdown
  })
}
