package de.up.hpi.informationsystems.adbms.benchmarks.performance

import java.io.{File, FileWriter, PrintWriter}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, ActorSystem, Cancellable, PoisonPill, Props}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.benchmarks.performance.InsertBenchmark.AddNewItemFunctor.Start
import de.up.hpi.informationsystems.adbms.protocols.{DefaultMessagingProtocol, RequestResponseProtocol}
import de.up.hpi.informationsystems.adbms.relation.Relation
import de.up.hpi.informationsystems.sampleapp.DataInitializer.LoadData
import de.up.hpi.informationsystems.sampleapp.dactors.SystemInitializer.{Shutdown, Startup}
import de.up.hpi.informationsystems.sampleapp.dactors.{Cart, Customer, GroupManager, StoreSection, SystemInitializer => SASystemInitializer}

import scala.concurrent.duration._
import scala.language.postfixOps


object InsertBenchmark extends App {
  println("Starting system")
  val actorSystem: ActorSystem = ActorSystem("benchmark-system")
  val initializer: ActorRef = actorSystem.actorOf(Props[SystemInitializer], "initializer")
  initializer ! Startup(15 seconds)

  sys.addShutdownHook({
    println("Received shutdown signal from JVM")
    initializer ! Shutdown
  })

  class SystemInitializer extends Actor with ActorLogging {
    import SASystemInitializer._

    val dataDir = "/data/resources/data_010_mb"

    val classNameDactorClassMap = Map(
      "Cart" -> classOf[Cart],
      "Customer" -> classOf[Customer],
      "GroupManager" -> classOf[GroupManager],
      "StoreSection" -> classOf[StoreSection]
    )

    def listDactor(sourceDir: File): (Class[_<: Dactor], Int) = {
      val folderName = sourceDir.getCanonicalPath.split(File.separatorChar).lastOption
      val dactorClassName = folderName.flatMap(_.split("-").headOption)
      val dactorId = folderName.flatMap(_.split("-").lastOption).map(_.toInt)

      (dactorClassName, dactorId) match {
        case (Some(className), Some(id)) => (classNameDactorClassMap(className), id)
        case _ => throw new RuntimeException(s"Could not parse folder name to dactor class name and id for: ($dactorClassName, $dactorId)")
      }
    }

    def initDactor(dactorInfo: (Class[_<: Dactor], Int)): ActorRef = {
      val dactor = Dactor.dactorOf(context.system, dactorInfo._1, dactorInfo._2)
      context.watch(dactor)
      dactor
    }

    def recursiveListDirs(d: File): List[File] = {
      val these = d.listFiles()
      these.filter(_.isDirectory).toList ++ these.filter(_.isDirectory).flatMap(recursiveListDirs)
    }

    ///// state machine
    override def receive: Receive = down orElse commonBehavior

    def down: Receive = {
      case Startup(timeout) =>
        log.info(s"Starting up system and loading data from resource root: $dataDir")

        val dataURL = getClass.getResource(dataDir)
        if(dataURL == null)
          throw new RuntimeException(s"Could not find resource root: $dataDir")
        val dirList = recursiveListDirs(new File(dataURL.getPath))

        val pendingACKs = dirList
          //.slice(0, 10)
          .map(listDactor)
          .toSet[(Class[_<: Dactor], Int)]  // put everything in a set to get rid of duplicates!
          .map(initDactor)
          .toSeq

        // get start time
        val startTime = System.nanoTime()

        // send message to all Dactors
        val loadDataMsg = LoadData(dataDir)
        println(s"Sending $loadDataMsg")
        pendingACKs.foreach( _ ! loadDataMsg )

        // schedule timeout
        import context.dispatcher
        val loadTimeout = context.system.scheduler.scheduleOnce(timeout, self, Timeout)

        println("Waiting for ACK")
        context.become(waitingForACK(pendingACKs, loadTimeout, startTime) orElse commonBehavior)
    }

    def waitingForACK(pendingACKs: Seq[ActorRef], timeout: Cancellable, startTime: Long): Receive = {
      case akka.actor.Status.Success =>
        log.info(s"Received ACK for data loading of $sender")
        val remainingACKs = pendingACKs.filterNot(_ == sender())

        if(remainingACKs.isEmpty) {
          val endTime: Long = System.nanoTime()
          log.info("finished startup")

          println("========== elapsed time (bulk data load) ==========")
          println(endTime-startTime)

          executeInsertCustomer(timeout)
        } else {
          context.become(waitingForACK(remainingACKs, timeout, startTime) orElse commonBehavior)
        }

      case akka.actor.Status.Failure(e) =>
        log.error(e, s"Could not initialize $sender")
        self ! Shutdown
    }

    def up: Receive = commonBehavior

    def waitingForItemInsertACKs(pendingACKs: Map[ActorRef, Long], timeout: Cancellable, finishedTimes: Seq[Long]): Receive = {
      case AddNewItemFunctor.Start.Success(_) =>
        log.info(s"Received ACK for adding customer $sender")

        // calculate runtime for this query
        val runtime = System.nanoTime() - pendingACKs(sender())
        val newFinishedTimes = finishedTimes :+ runtime
        val remainingACKs = pendingACKs.filterNot(p => p._1 == sender())

        if (remainingACKs.isEmpty) {
          val endTime = System.nanoTime()
          timeout.cancel()

          val filename: String = "results.txt"
          println("========== elapsed time (insert) ==========")
          println(s"Collected ${newFinishedTimes.size} measurements")

          val resultString = newFinishedTimes.mkString("", "\n", "")
          val fw = new FileWriter(filename, false)
          fw.write(resultString)
          fw.close()

          println(s"Results written to $filename")

          handleShutdown()
        } else {
          context.become(waitingForItemInsertACKs(remainingACKs, timeout, newFinishedTimes) orElse commonBehavior)
        }
      case message => println(s"received $message")
    }

    def commonBehavior: Receive = {
      case Timeout =>
        log.error("System startup timed-out")
        handleShutdown()

      case Shutdown => handleShutdown()
    }
    /////

    def handleShutdown(): Unit = {
      log.info("Shutting down system!")
      context.children.foreach( _ ! PoisonPill )
      context.stop(self)
      context.system.terminate()
    }

    def executeInsertCustomer(timeout: Cancellable): Unit = {
      // start concurrent functors for adding items to storesections and their discounts to groupmanagers
      val numInserts: Int = 1000

      val items = List.range(0, numInserts)
      val functors = items.map(i => (i, InsertBenchmark.actorSystem.actorOf(Props[AddNewItemFunctor], s"addNewCustomer-functor-$i")))
      val pendingACKs = functors.map({ case (i: Int, ref: ActorRef) =>
        val startTime = System.nanoTime()
        ref ! AddNewItemFunctor.Start.Request(100000 + i, i % 10, Map(i % 5 -> 0.5))
        (ref, startTime)
      })

      println("Waiting for ACK")
      context.become(waitingForItemInsertACKs(pendingACKs.toMap, timeout, Seq.empty) orElse commonBehavior)
    }
  }

  object AddNewItemFunctor {
    object Start {
      sealed trait Start extends RequestResponseProtocol.Message
      case class Request(itemId: Int, storeId: Int, discounts: Map[Int, Double]) extends RequestResponseProtocol.Request[Start]
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
          StoreSection.Inventory.inventoryId ~> 21
        ).build()))

        val groupManager: ActorSelection = Dactor.dactorSelection(context.system, classOf[GroupManager], discounts.keys.head)
        groupManager ! DefaultMessagingProtocol.InsertIntoRelation("discounts", Seq(GroupManager.Discounts.newRecord(
          GroupManager.Discounts.fixedDisc ~> 0.4
        ).build()))

        context.become(waitingForACKs(2, sender()))
    }

    def waitingForACKs(count: Int, parent: ActorRef): Receive = {
      case akka.actor.Status.Success =>
        if (count == 1) {
          parent ! Start.Success(Relation.empty)
          println(s"finished with ${context.self}")
          context.stop(self)
        } else {
          context.become(waitingForACKs(count - 1, parent))
        }
    }
  }

}