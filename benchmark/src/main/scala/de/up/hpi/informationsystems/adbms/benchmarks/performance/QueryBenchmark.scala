package de.up.hpi.informationsystems.adbms.benchmarks.performance

import akka.actor.{Actor, ActorIdentity, ActorLogging, ActorRef, ActorSystem, Cancellable, Identify, Props}
import de.up.hpi.informationsystems.adbms.benchmarks.performance.BulkInsertBenchmark.{SystemInitializer => BISystemInitializer}
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol.Response
import de.up.hpi.informationsystems.adbms.relation.{Relation, RelationBinOps}
import de.up.hpi.informationsystems.sampleapp.dactors.StoreSection.GetAvailableQuantityFor
import de.up.hpi.informationsystems.sampleapp.dactors.StoreSection.GetAvailableQuantityFor.GetAvailableQuantityFor
import de.up.hpi.informationsystems.sampleapp.dactors.SystemInitializer.Startup
import de.up.hpi.informationsystems.sampleapp.dactors.{StoreSection, SystemInitializer => SASystemInitializer}

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
          resolveStoreSectionIdentities()
        }(remainingACKs =>
          context.become(waitingForACK2(remainingACKs, timeout) orElse commonBehavior)
        )

      case akka.actor.Status.Failure(e) =>
        log.error(e, s"Could not initialize $sender")
        self ! Shutdown
    }

    def waitForIdentities(storeSections: Seq[ActorRef]): Receive = {
      case ActorIdentity(_, Some(ref)) =>
        context.become(waitForIdentities(storeSections :+ ref))

      case ActorIdentity(_, None) =>
        println("no actor found")

      case Timeout =>
        println("start measuring queries")
        val functor = context.actorOf(Props[ScanQueryFunctor])
        functor ! Start(storeSections)
        context.become(waitForQueryResult())
    }

    def waitForQueryResult(): Receive = {
      case rel: Relation =>
        println("Received results")
        println(rel)
        handleShutdown()
    }

    def resolveStoreSectionIdentities(): Unit = {
      println("Resolving store section identities")
      val storeSections = context.actorSelection(s"/user/${classOf[StoreSection].getSimpleName}-*")
      storeSections ! Identify(1)
      // schedule timeout
      import context.dispatcher
      context.system.scheduler.scheduleOnce(3 seconds, self, Timeout)
      context.become(waitForIdentities(Seq.empty) orElse commonBehavior)
    }
  }

  case class Start(storeSections: Seq[ActorRef])

  class ScanQueryFunctor extends Actor with ActorLogging {

    override def receive: Receive = sendRequest

    def sendRequest: Receive = {
      case Start(storeSections) =>
        log.info(s"Started scan query functor with ${storeSections.length} target actors")
        storeSections.foreach(_ ! GetAvailableQuantityFor.Request(115))
        context.become(waitForResults(sender, storeSections, Seq.empty))
    }

    def waitForResults(receiver: ActorRef, pending: Seq[ActorRef], results: Seq[Response[GetAvailableQuantityFor]]): Receive = {
      case result: Response[GetAvailableQuantityFor] =>
        val newPending = pending.filterNot(_ == sender)
        if (newPending.isEmpty) {
          log.info("Received all results of scan query, sending result to receiver")
          val concatenatedResult = results
            .flatMap {
              case GetAvailableQuantityFor.Success(res) => Some(res)
              case GetAvailableQuantityFor.Failure(_) => None
            }
            .reduce(RelationBinOps.union)
          receiver ! concatenatedResult
          context.stop(self)

        } else
          context.become(waitForResults(receiver, newPending, results :+ result))
    }
  }
}
