package de.up.hpi.informationsystems.adbms.benchmarks

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Cancellable, PoisonPill, Props}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.sampleapp.DataInitializer.LoadData
import de.up.hpi.informationsystems.sampleapp.dactors.{Cart, Customer, GroupManager, StoreSection}

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration


object DactorBenchmark extends App {
  println("Starting system")
  SystemInitializer.initializer ! SystemInitializer.Startup(15 seconds)

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      println(s"Received shutdown signal from JVM")
      SystemInitializer.initializer ! SystemInitializer.Shutdown
    }
  })
}


object SystemInitializer {

  final case class Startup(timeout: FiniteDuration)

  final case object Shutdown

  final case object Timeout

  def props: Props = Props(new SystemInitializer())

  /**
    * Returns the benchmark's actor system
    */
  // only instantiated once (first call) as we are in a `object` and have a `lazy val`
  lazy val actorSystem: ActorSystem = ActorSystem("benchmark-system")

  /**
    * Returns the ActorRef of the initializer actor
    */
  // only instantiated once (first call) as we are in a `object` and have a `lazy val`
  lazy val initializer: ActorRef = actorSystem.actorOf(props, "initializer")

}

class SystemInitializer extends Actor with ActorLogging {
  import SystemInitializer._

  val dataDir = "/data"

  ///// state machine
  override def receive: Receive = down orElse commonBehavior

  def down: Receive = {
    case Startup(timeout) =>
      log.info(s"Starting up system and loading data from resource root: $dataDir")

      val dataURL = getClass.getResource(dataDir)
      println(dataURL)
      val dirList = recursiveListDirs(new File(dataURL.getPath))

      val pendingACKs = dirList
        //.slice(0, 10)
        .map(initDactor)

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

  val classNameDactorClassMap = Map(
    "Cart" -> classOf[Cart],
    "Customer" -> classOf[Customer],
    "GroupManager" -> classOf[GroupManager],
    "StoreSection" -> classOf[StoreSection]
  )

  def initDactor(sourceDir: File): ActorRef = {
    val folderName = sourceDir.getCanonicalPath.split(File.separatorChar).last
    val dactorClassName = folderName.split("-").head
    val dactorClass = classNameDactorClassMap(dactorClassName)
    val dactorId = folderName.split("-").last.toInt

    val dactor = Dactor.dactorOf(context.system, dactorClass, dactorId)
    context.watch(dactor)
    dactor
  }

  def recursiveListDirs(d: File): List[File] = {
    val these = d.listFiles()
    these.filter(_.isDirectory).toList ++ these.filter(_.isDirectory).flatMap(recursiveListDirs)
  }

  // FIXME delete this?
  def recursiveListFiles(d: File): List[File] = {
    val these = d.listFiles()
    these.filter(_.isFile).toList ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def waitingForACK(pendingACKs: Seq[ActorRef], timeout: Cancellable): Receive = {
    case akka.actor.Status.Success =>
      log.info(s"Received ACK for data loading of $sender")
      val remainingACKs = pendingACKs.filterNot(_ == sender())

      if(remainingACKs.isEmpty) {
        log.info("finished startup")
        timeout.cancel()
        context.become(up orElse commonBehavior)
      } else {
        context.become(waitingForACK(remainingACKs, timeout) orElse commonBehavior)
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
    self ! PoisonPill
    context.system.terminate()
  }
}
