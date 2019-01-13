package de.up.hpi.informationsystems.adbms.benchmarks.performance

import java.io.FileWriter

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, ActorSystem, Cancellable, Props}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.benchmarks.performance.BulkInsertBenchmark.{SystemInitializer => BISystemInitializer}
import de.up.hpi.informationsystems.adbms.benchmarks.performance.Implicits._
import de.up.hpi.informationsystems.adbms.benchmarks.performance.InsertBenchmark.AddNewItemFunctor.Start
import de.up.hpi.informationsystems.adbms.protocols.{DefaultMessagingProtocol, RequestResponseProtocol}
import de.up.hpi.informationsystems.adbms.relation.Relation
import de.up.hpi.informationsystems.sampleapp.dactors.SystemInitializer.Startup
import de.up.hpi.informationsystems.sampleapp.dactors.{GroupManager, StoreSection, SystemInitializer => SASystemInitializer}

import scala.concurrent.duration._
import scala.language.postfixOps


object InsertBenchmark extends App {

  val N_INSERTS = 1000
  val STORE_ID_RANGE_END = 500
  val GROUP_MANAGER_ID_RANGE_END = 10
  val STARTUP_TIMEOUT = 15 seconds
  val DATASET = "data_050_mb"

  println("Starting system")
  val actorSystem: ActorSystem = ActorSystem("benchmark-system")
  val initializer: ActorRef = actorSystem.actorOf(Props[SystemInitializer], "initializer")
  initializer ! Startup(STARTUP_TIMEOUT)

  class SystemInitializer extends BISystemInitializer {
    import SASystemInitializer._

    override val dataDir = s"/data/resources/$DATASET"

    override def down: Receive = {
      case Startup(timeout) =>
        val pendingACKs = startActors()
        loadDataIntoActors(pendingACKs)

        // schedule timeout
        import context.dispatcher
        val loadTimeout = context.system.scheduler.scheduleOnce(timeout, self, Timeout)

        println("Waiting for ACK")
        context.become(waitingForACK(pendingACKs, loadTimeout) orElse commonBehavior)
    }

    def waitingForACK(pendingACKs: Seq[ActorRef], timeout: Cancellable): Receive = {
      case akka.actor.Status.Success =>
        checkACKs(pendingACKs, sender){
          log.info("finished startup")
          executeInsertCustomer(timeout)

        }(remainingACKs =>
          context.become(waitingForACK(remainingACKs, timeout) orElse commonBehavior)
        )

      case akka.actor.Status.Failure(e) =>
        log.error(e, s"Could not initialize $sender")
        self ! Shutdown
    }

    def waitingForItemInsertACKs(pendingACKs: Map[ActorRef, Long], timeout: Cancellable, finishedTimes: Seq[Long], startTime: Long): Receive = {
      case AddNewItemFunctor.Start.Success(_) =>
        log.info(s"Received ACK for adding customer $sender")

        // calculate runtime for this query
        val runtime = System.nanoTime() - pendingACKs(sender())
        val newFinishedTimes = finishedTimes :+ runtime
        val remainingACKs = pendingACKs.filterNot(p => p._1 == sender())

        if (remainingACKs.isEmpty) {
          val endTime = System.nanoTime()
          timeout.cancel()

          val overallRuntime = endTime-startTime

          println("========== elapsed time (insert) ==========")
          println(s"Inserted ${newFinishedTimes.size} x 2 (2 Dactors) rows in $overallRuntime ns")
          println(s"Throughput: ${N_INSERTS/(overallRuntime*1e-9)} Op/s")
          println(s"Average latency: ${newFinishedTimes.avg()/1e6} ms")
          println(s"Median latency: ${newFinishedTimes.median()/1e6} ms")

          val filename: String = s"insert-results-$DATASET-s$STORE_ID_RANGE_END-g$GROUP_MANAGER_ID_RANGE_END.txt"
          val resultString = newFinishedTimes.mkString("", "\n", "")
          val fw = new FileWriter(filename, false)
          fw.write("========== elapsed time (insert) ==========\n")
          fw.write(s"Inserted ${newFinishedTimes.size} x 2 (2 Dactors) rows in $overallRuntime ns \n")
          fw.write(s"Throughput: ${N_INSERTS/(overallRuntime*1e-9)} Op/s\n")
          fw.write(s"Average latency: ${newFinishedTimes.avg()/1e6} ms\n")
          fw.write(s"Median latency: ${newFinishedTimes.median()/1e6} ms\n")
          fw.write("========== single results (ns) ===========\n")
          fw.write(resultString)
          fw.close()

          println(s"Results written to $filename")

          handleShutdown()
        } else {
          context.become(waitingForItemInsertACKs(remainingACKs, timeout, newFinishedTimes, startTime) orElse commonBehavior)
        }
      case message => println(s"received $message")
    }
    /////

    def executeInsertCustomer(timeout: Cancellable): Unit = {
      // start concurrent functors for adding items to storesections and their discounts to groupmanagers
      val startTime: Long = System.nanoTime()

      val pendingACKs = (0 until N_INSERTS).map( i => {
        val startTime = System.nanoTime()
        val ref = InsertBenchmark.actorSystem.actorOf(Props[AddNewItemFunctor], s"addNewCustomer-functor-$i")
        ref ! AddNewItemFunctor.Start.Request(100000 + i, i % STORE_ID_RANGE_END, i % GROUP_MANAGER_ID_RANGE_END -> 0.5)
        ref -> startTime
      })

      println("Waiting for insert ACKs")
      context.become(waitingForItemInsertACKs(pendingACKs.toMap, timeout, Seq.empty, startTime) orElse commonBehavior)
    }
  }

  object AddNewItemFunctor {
    object Start {
      sealed trait Start extends RequestResponseProtocol.Message
      case class Request(itemId: Int, storeId: Int, discounts: (Int, Double)) extends RequestResponseProtocol.Request[Start]
      case class Success(result: Relation) extends RequestResponseProtocol.Success[Start]
      case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[Start]
    }
  }

  class AddNewItemFunctor extends Actor with ActorLogging {
    import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._

    override def receive: Receive = down

    def down: Receive = {
      case Start.Request(itemId, storeSectionId, discounts) =>
        val storeSection: ActorSelection = Dactor.dactorSelection(context.system, classOf[StoreSection], storeSectionId)
        storeSection ! DefaultMessagingProtocol.InsertIntoRelation("inventory", Seq(StoreSection.Inventory.newRecord(
          StoreSection.Inventory.quantity ~> 10 &
          StoreSection.Inventory.price ~> 2.50 &
          StoreSection.Inventory.inventoryId ~> itemId
        ).build()))

        val groupManager: ActorSelection = Dactor.dactorSelection(context.system, classOf[GroupManager], discounts._1)
        groupManager ! DefaultMessagingProtocol.InsertIntoRelation("discounts", Seq(GroupManager.Discounts.newRecord(
          GroupManager.Discounts.fixedDisc ~> discounts._2
        ).build()))

        context.become(waitingForACKs(2, sender()))
    }

    def waitingForACKs(count: Int, parent: ActorRef): Receive = {
      case akka.actor.Status.Success =>
        if (count == 1) {
          parent ! Start.Success(Relation.empty)
          log.info(s"finished with ${context.self}")
          context.stop(self)
        } else {
          context.become(waitingForACKs(count - 1, parent))
        }
    }
  }

}