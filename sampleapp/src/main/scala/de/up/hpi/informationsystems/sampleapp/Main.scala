package de.up.hpi.informationsystems.sampleapp

import de.up.hpi.informationsystems.sampleapp.dactors.SystemInitializer

import scala.concurrent.duration._
import scala.language.postfixOps

object Main extends App {

  // to allow using Main in CI: shutdown system after a specific time
  SystemInitializer.actorSystem.scheduler.scheduleOnce(
    10 seconds, SystemInitializer.initializer, SystemInitializer.Shutdown
  )(
    SystemInitializer.actorSystem.dispatcher, akka.actor.Actor.noSender
  )

  println("Starting system")
  SystemInitializer.initializer ! SystemInitializer.Startup(5 seconds)

  sys.addShutdownHook({
    println(s"Received shutdown signal from JVM")
    SystemInitializer.initializer ! SystemInitializer.Shutdown
  })

}