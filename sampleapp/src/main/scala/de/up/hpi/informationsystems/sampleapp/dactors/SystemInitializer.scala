package de.up.hpi.informationsystems.sampleapp.dactors

import java.net.URI

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Cancellable, PoisonPill, Props}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.sampleapp.DataInitializer.LoadData

import scala.concurrent.duration._
import scala.language.postfixOps


object SystemInitializer {

  final case object Startup

  final case object Shutdown

  final case object Timeout

  def props: Props = Props(new SystemInitializer())

  lazy val actorSystem: ActorSystem = ActorSystem("sampleapp-system")

  lazy val initializer: ActorRef = actorSystem.actorOf(props, "initializer")

}

class SystemInitializer extends Actor with ActorLogging {
  import SystemInitializer._

  ///// state machine
  override def receive: Receive = down orElse commonBehavior

  def down: Receive = {
    case Startup =>
      log.info(s"Starting up system and loading data from root: $manifestUri")
      val storeSection14 = Dactor.dactorOf(context.system, classOf[StoreSection], 14)
      storeSection14 ! LoadData(manifestUri)
      context.watch(storeSection14)
      val pendingACKs = Seq(storeSection14)

      import context.dispatcher
      val loadTimeout = context.system.scheduler.scheduleOnce(20 seconds, self, Timeout)

      context.become(waitingForACK(pendingACKs, loadTimeout) orElse commonBehavior)
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

  def manifestUri: URI = new URI(getClass.getResource("/data/manifest").toString)
}
