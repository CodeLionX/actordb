package de.up.hpi.informationsystems.adbms.benchmarks.performance

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Cancellable, PoisonPill, Props}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.sampleapp.DataInitializer.LoadData
import de.up.hpi.informationsystems.sampleapp.dactors.SystemInitializer.{Shutdown, Startup}
import de.up.hpi.informationsystems.sampleapp.dactors.{Cart, Customer, GroupManager, StoreSection, SystemInitializer => SASystemInitializer}

import scala.concurrent.duration._
import scala.language.postfixOps


object BulkInsertBenchmark extends App {
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
          val endTime = System.nanoTime()
          log.info("finished startup")
          timeout.cancel()

          println("========== elapsed time ==========")
          println(endTime-startTime)
          handleShutdown()
        } else {
          context.become(waitingForACK(remainingACKs, timeout, startTime) orElse commonBehavior)
        }

      case akka.actor.Status.Failure(e) =>
        log.error(e, s"Could not initialize $sender")
        self ! Shutdown
    }

    def up: Receive = commonBehavior

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
  }

}