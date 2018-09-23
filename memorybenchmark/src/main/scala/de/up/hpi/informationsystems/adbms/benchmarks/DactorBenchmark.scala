package de.up.hpi.informationsystems.adbms.benchmarks

import java.io.File

import akka.actor._
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.sampleapp.DataInitializer.LoadData
import de.up.hpi.informationsystems.sampleapp.dactors.SystemInitializer.{Shutdown, Startup}
import de.up.hpi.informationsystems.sampleapp.dactors.{Cart, Customer, GroupManager, StoreSection, SystemInitializer => SASystemInitializer}

import scala.concurrent.duration._
import scala.language.postfixOps


object DactorBenchmark extends App {
  println("Starting system")
  val actorSystem: ActorSystem = ActorSystem("benchmark-system")
  val initializer: ActorRef = actorSystem.actorOf(Props[SystemInitializer], "initializer")
  initializer ! Startup(15 seconds)

  sys.addShutdownHook({
    println("Received shutdown signal from JVM")
    initializer ! Shutdown
  })
}


class SystemInitializer extends SASystemInitializer {
  import SASystemInitializer._

  val dataDir = "/data/loadtest/data_010_mb"

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

  override def down: Receive = {
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

      // send message to all Dactors
      val loadDataMsg = LoadData(dataDir)
      println(s"Sending $loadDataMsg")
      pendingACKs.foreach( _ ! loadDataMsg )

      // schedule timeout
      import context.dispatcher
      val loadTimeout = context.system.scheduler.scheduleOnce(timeout, self, Timeout)

      println("Waiting for ACK")
      context.become(waitingForACK(pendingACKs, loadTimeout) orElse commonBehavior)
  }
}
