package de.up.hpi.informationsystems.adbms.benchmarks.performance

import java.io.FileWriter

import akka.actor.{Actor, ActorIdentity, ActorLogging, ActorRef, ActorSystem, Cancellable, Identify, Props}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.benchmarks.performance.BulkInsertBenchmark.{SystemInitializer => BISystemInitializer}
import de.up.hpi.informationsystems.adbms.protocols.DefaultMessagingProtocol.SelectAllFromRelation
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol.Response
import de.up.hpi.informationsystems.adbms.relation.{Relation, RelationBinOps}
import de.up.hpi.informationsystems.sampleapp.dactors.StoreSection.GetAvailableQuantityFor
import de.up.hpi.informationsystems.sampleapp.dactors.StoreSection.GetAvailableQuantityFor.GetAvailableQuantityFor
import de.up.hpi.informationsystems.sampleapp.dactors.SystemInitializer.Startup
import de.up.hpi.informationsystems.sampleapp.dactors.{StoreSection, SystemInitializer => SASystemInitializer}

import scala.concurrent.duration._
import scala.language.postfixOps


object QueryBenchmark extends App {

  val N_QUERIES = 1000
  val N_POINTQUERIES = N_QUERIES
  val DATASET = "data_050_mb"
  val IDENTITY_RESOLVE_TIMEOUT = 2 seconds
  val STARTUP_TIMEOUT = 15 seconds

  println("Starting system")
  val actorSystem: ActorSystem = ActorSystem("benchmark-system")
  val initializer: ActorRef = actorSystem.actorOf(Props[SystemInitializer], "initializer")
  initializer ! Startup(STARTUP_TIMEOUT)

  class SystemInitializer extends BISystemInitializer {
    import SASystemInitializer._

    override val dataDir = s"/data/resources/$DATASET"

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
        startScanQueries(storeSections, N_QUERIES)
    }

    def waitForQueryResult(pendingACKs: Map[ActorRef, Long], finishedTimes: Seq[Long], startTime: Long): Receive = {
      case _: Relation =>
        val runtime = System.nanoTime() - pendingACKs(sender)
        val newFinishedTimes = finishedTimes :+ runtime
        val newPendingACKs = pendingACKs.filterNot( t => t._1 == sender() )

        if(newPendingACKs.isEmpty) {
          val endTime = System.nanoTime()

          val filename: String = "scan-query-results.txt"
          println("========== elapsed time (scan query) ==========")
          println(s"elapsed nanos: ${endTime - startTime}ns (${(endTime - startTime) / 1000000000}s)")
          println(s"average time per query: ${newFinishedTimes.avg()}ns")
          println(s"Collected ${newFinishedTimes.size} measurements")

          val resultString = newFinishedTimes.mkString("", "\n", "")
          val fw = new FileWriter(filename, false)
          fw.write(resultString)
          fw.close()

          println(s"Results written to $filename")
          println()

          // measure point queries
          val newStartTime = startPointQuery()
          context.become(waitForPointQueryResult(N_POINTQUERIES, newStartTime, Seq.empty))
        } else {
          context.become(waitForQueryResult(newPendingACKs, newFinishedTimes, startTime))
        }
    }

    def waitForPointQueryResult(remainingQueries: Int, started: Long, finishedTimes: Seq[Long]): Receive = {
      case SelectAllFromRelation.Success(_) =>
        val endTime = System.nanoTime()
        val newFinishedTimes = finishedTimes :+ (endTime - started)
        val newRemainingQueries = remainingQueries-1

        if(newRemainingQueries == 0) {
          val filename: String = "point-query-results.txt"
          println("========== elapsed time (point query) ==========")
          println(s"median latency per query: ${newFinishedTimes.median()}ns")
          println(s"Collected ${newFinishedTimes.size} measurements")

          val resultString = newFinishedTimes.mkString("", "\n", "")
          val fw = new FileWriter(filename, false)
          fw.write(resultString)
          fw.close()

          println(s"Results written to $filename")
          println()
          handleShutdown()

        } else {
          val newStartTime = startPointQuery()
          context.become(waitForPointQueryResult(newRemainingQueries, newStartTime, newFinishedTimes))
        }
    }

    def resolveStoreSectionIdentities(): Unit = {
      println("Resolving store section identities")
      val storeSections = context.actorSelection(s"/user/${classOf[StoreSection].getSimpleName}-*")
      storeSections ! Identify(1)
      // schedule timeout
      import context.dispatcher
      context.system.scheduler.scheduleOnce(IDENTITY_RESOLVE_TIMEOUT, self, Timeout)
      context.become(waitForIdentities(Seq.empty) orElse commonBehavior)
    }

    def startScanQueries(storeSections: Seq[ActorRef], n: Int): Unit = {
      val startTime = System.nanoTime()
      val pending = (0 until n).map( _ => {
        val begin = System.nanoTime()
        val functor = context.actorOf(Props[ScanQueryFunctor[GetAvailableQuantityFor]])
        functor ! Start(storeSections, GetAvailableQuantityFor.Request(115))
        functor -> begin
      }).toMap
      context.become(waitForQueryResult(pending, Seq.empty, startTime))
    }

    def startPointQuery(): Long = {
      val query = SelectAllFromRelation.Request(StoreSection.Inventory.name)
      val selection = Dactor.dactorSelection(context, classOf[StoreSection], 115)

      val startTime = System.nanoTime()
      selection ! query
      startTime
    }
  }

  case class Start(dactors: Seq[ActorRef], message: RequestResponseProtocol.Request[RequestResponseProtocol.Message])

  class ScanQueryFunctor[T <: RequestResponseProtocol.Message] extends Actor with ActorLogging {

    override def receive: Receive = sendRequest

    def sendRequest: Receive = {
      case Start(dactors, message) =>
        log.info(s"Started scan query functor with ${dactors.length} target dactors")
        dactors.foreach(_ ! message)
        context.become(waitForResults(sender, dactors, Seq.empty))
    }

    def waitForResults(receiver: ActorRef, pending: Seq[ActorRef], results: Seq[Response[T]]): Receive = {
      case result: Response[T] =>
        val newPending = pending.filterNot(_ == sender)
        if (newPending.isEmpty) {
          log.info("Received all results of scan query, sending result to receiver")
          val results2 = results
            .flatMap {
              case res: RequestResponseProtocol.Success[T] => Some(res.result)
              case _: RequestResponseProtocol.Failure[T] => None
            }
          val concatenatedResult =
            if(results2.nonEmpty) results2.reduce(RelationBinOps.union)
            else Relation.empty
          receiver ! concatenatedResult
          context.stop(self)

        } else
          context.become(waitForResults(receiver, newPending, results :+ result))
    }
  }

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
