package de.up.hpi.informationsystems.fouleggs.dactors

import java.time.{ZoneId, ZonedDateTime}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Cancellable, PoisonPill, Props}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.protocols.DefaultMessagingProtocol.InsertIntoRelation
import de.up.hpi.informationsystems.adbms.record.Record

import scala.concurrent.duration.FiniteDuration

object SystemInitializer {

  final case class Startup(timeout: FiniteDuration)

  final case object Shutdown

  final case object Timeout

  def props: Props = Props(new SystemInitializer())

  /**
    * Returns the sampleapp's actor system
    */
  // only instantiated once (first call) as we are in a `object` and have a `lazy val`
  lazy val actorSystem: ActorSystem = ActorSystem("fouleggs-system")

  /**
    * Returns the ActorRef of the initializer actor
    */
  // only instantiated once (first call) as we are in a `object` and have a `lazy val`
  lazy val initializer: ActorRef = actorSystem.actorOf(props, "initializer")
}

class SystemInitializer extends Actor with ActorLogging {
  import SystemInitializer._
  import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._

  override def receive: Receive = down orElse commonBehavior

  def down: Receive = {
    case Startup(timeout) =>
      val empireStrikesBack = Dactor.dactorOf(context.system, classOf[Film], 1)
      context.watch(empireStrikesBack)

      val markHamill = Dactor.dactorOf(context.system, classOf[Person], 1)
      context.watch(markHamill)

      val empireInfoRec: Record = Film.Info.newRecord(
        Film.Info.title ~> "The Empire Strikes Back" &
        Film.Info.release ~> ZonedDateTime.of(1980, 5, 17, 0, 0, 0, 0, ZoneId.of("UTC-08:00")) &
        Film.Info.description ~> "After the rebels are brutally overpowered by the Empire on the ice planet Hoth, Luke Skywalker begins Jedi training with Yoda, while his friends are pursued by Darth Vader."
      ).build()

      val markInfo: Record = Person.Info.newRecord(
        Person.Info.firstName ~> "Mark" &
        Person.Info.lastName ~> "Hamill"  &
        Person.Info.birthday ~> ZonedDateTime.of(1951, 9, 25, 0, 0, 0, 0, ZoneId.of("UTC-08:00"))
      ).build()

      // send message to all Dactors
      val pendingACKs = Seq(empireStrikesBack, markHamill)
      empireStrikesBack ! InsertIntoRelation("film_info", Seq(empireInfoRec))
      markHamill ! InsertIntoRelation("person_info", Seq(markInfo))

      // schedule timeout
      import context.dispatcher
      val loadTimeout = context.system.scheduler.scheduleOnce(timeout, self, Timeout)

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

  def handleShutdown(): Unit = {
    log.info("Shutting down system!")
    context.children.foreach( _ ! PoisonPill )
    self ! PoisonPill
    context.system.terminate()
  }
}
