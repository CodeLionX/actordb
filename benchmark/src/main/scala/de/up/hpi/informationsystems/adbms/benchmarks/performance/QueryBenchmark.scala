package de.up.hpi.informationsystems.adbms.benchmarks.performance

import akka.actor.{ActorRef, ActorSystem, Cancellable, Props}
import de.up.hpi.informationsystems.adbms.benchmarks.performance.BulkInsertBenchmark.{SystemInitializer => BISystemInitializer}
import de.up.hpi.informationsystems.sampleapp.dactors.SystemInitializer.Startup
import de.up.hpi.informationsystems.sampleapp.dactors.{SystemInitializer => SASystemInitializer}

import scala.concurrent.duration._
import scala.language.postfixOps


object QueryBenchmark extends App {
  println("Starting system")
  val actorSystem: ActorSystem = ActorSystem("benchmark-system")
  val initializer: ActorRef = actorSystem.actorOf(Props[SystemInitializer], "initializer")
  initializer ! Startup(15 seconds)

  class SystemInitializer extends BISystemInitializer {
    import SASystemInitializer._

    override def down: Receive = {
      case Startup(timeout) =>
        // start system and load data
        val pendingACKs = startActors()
        loadDataIntoActors(pendingACKs)

        // schedule timeout
        import context.dispatcher
        val loadTimeout = context.system.scheduler.scheduleOnce(timeout, self, Timeout)

        println("Waiting for ACK")
        context.become(waitingForACK2(pendingACKs, loadTimeout) orElse commonBehavior)
    }

    def waitingForACK2(pendingACKs: Seq[ActorRef], timeout: Cancellable): Receive = {
      case akka.actor.Status.Success =>
        checkACKs(pendingACKs, sender){
          measureDotQueries()
        }(remainingACKs =>
          context.become(waitingForACK2(remainingACKs, timeout) orElse commonBehavior)
        )

      case akka.actor.Status.Failure(e) =>
        log.error(e, s"Could not initialize $sender")
        self ! Shutdown
    }

    def measureDotQueries() = {
      println("start measuring queries")
      handleShutdown()
    }
  }
}
