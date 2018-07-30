package de.up.hpi.informationsystems.fouleggs.dactors

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.protocols.DefaultMessagingProtocol
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.relation.Relation

object AdminSession {

  final case class Up()

  object AddCastToFilm {
    final case class Request(personId: Int, filmId: Int, roleName: String)
    final case class Success(personId: Int, filmId: Int, roleName: String)
    final case class Failure(e: Throwable)
  }

  // :addFilm(filmName, filmDescription, filmRelease)
  // :addPerson(firstName, lastName birthDay)

  // Core functionality for MultiDactorFunction impl:
  // :addCastToFilm(personId, filmId, roleName) <--- MultiDactorFunction! // Range query on non-optimally layed out data: // :findPerson(freeText: firstName andor lastName andor birthday)

  // Only for consideration:
  // :find(freeText: Person.firstName or Film.Info.title or Film.Cast.roleName etc)

  def props: Props = Props[AdminSession]
}

/**
  * Provides top level functionalities
  */
class AdminSession extends Actor with ActorLogging {
  override def receive: Receive = commonBehaviour

  def commonBehaviour: Receive = {
    case AdminSession.Up => sender() ! akka.actor.Status.Success
    case AdminSession.AddCastToFilm.Request(personId, filmId, roleName) =>
      addCastToFilm(personId, filmId, roleName)
  }

  def addCastToFilm(personId: Int, filmId: Int, roleName: String): Unit = {
    val functor: ActorRef = context.system.actorOf(Props(new CastAndFilmographyFunctor(personId, filmId, roleName, self)))
    context.become(waitingForSuccess(functor))

    /*
    Nested MultiDactorFunction:
    ConcurrentFunction wrapping two SequentialFunctions:
    1) Person.GetInfo -> Film.AddCast
    2) Film.GetInfo -> Person.AddFilm
     */
  }

  def waitingForSuccess(from: ActorRef): Receive = {
    case akka.actor.Status.Success if sender == from => {
      log.info("SUCCCCCCESS")


    }
  }
}

class CastAndFilmographyFunctor(personId: Int, filmId: Int, roleName: String, backTo: ActorRef) extends Actor {

  val sub1: ActorRef = context.system.actorOf(Props(new AddFilmFunctor(personId, filmId, roleName, self)))
  val sub2: ActorRef = context.system.actorOf(Props(new AddCastFunctor(personId, filmId, roleName, self)))

  override def receive: Receive = waitingForAck(Seq(sub1, sub2))

  def waitingForAck(pending: Seq[ActorRef]): Receive = {
    case akka.actor.Status.Success =>
      val remainingACKs = pending.filterNot(_ == sender())

      if(remainingACKs.isEmpty) {
        backTo ! akka.actor.Status.Success
        context.stop(self)
      } else {
        context.become(waitingForAck(remainingACKs))
      }

    case akka.actor.Status.Failure(e) =>
      backTo ! akka.actor.Status.Failure(e)
      context.stop(self)
  }
}

class AddFilmFunctor(personId: Int, filmId: Int, roleName: String, backTo: ActorRef) extends Actor {

  override def receive: Receive = waitingForFilmInfo orElse commonBehaviour

  Dactor.dactorSelection(context.system, classOf[Film], filmId) ! DefaultMessagingProtocol.SelectAllFromRelation.Request("film_info")

  def waitingForFilmInfo: Receive = {
    case DefaultMessagingProtocol.SelectAllFromRelation.Failure(e) => fail(e)
    case DefaultMessagingProtocol.SelectAllFromRelation.Success(relation: Relation) => {
      context.become(waitingForInsertAck orElse commonBehaviour)

      val filmInfo: Record = relation.records.get.head

      val newFilmRecord: Record = Person.Filmography.newRecord(
        Person.Filmography.filmId ~> filmId &
        Person.Filmography.roleName ~> roleName &
        Person.Filmography.filmName ~> filmInfo.get(Film.Info.title).get &
        Person.Filmography.filmRelease ~> filmInfo.get(Film.Info.release).get
      ).build()
      Dactor.dactorSelection(context.system, classOf[Person], personId) ! DefaultMessagingProtocol.InsertIntoRelation("filmography", Seq(newFilmRecord))
    }
  }

  def waitingForInsertAck: Receive = {
    case akka.actor.Status.Success => {
      backTo ! akka.actor.Status.Success
      context.stop(self) // because this is our last state
    }
  }

  def commonBehaviour: Receive = {
    case akka.actor.Status.Failure(e) => fail(e)
  }

  private def fail(e: Throwable): Unit = {
    backTo ! akka.actor.Status.Failure(e)
    context.stop(self)
  }
}

class AddCastFunctor(personId: Int, filmId: Int, roleName: String, backTo: ActorRef) extends Actor {

  override def receive: Receive = waitingForPersonInfo orElse commonBehaviour

  // very first message has to be sent outside of Receives
  Dactor.dactorSelection(context.system, classOf[Person], personId) ! DefaultMessagingProtocol.SelectAllFromRelation.Request("person_info")

  def waitingForPersonInfo: Receive = {
    case DefaultMessagingProtocol.SelectAllFromRelation.Failure(e) => fail(e)
    case DefaultMessagingProtocol.SelectAllFromRelation.Success(relation: Relation) => {
      context.become(waitingForInsertAck orElse commonBehaviour)

      val personInfo: Record = relation.records.get.head

      val newCastRecord: Record = Film.Cast.newRecord(
        Film.Cast.firstName ~> personInfo.get(Person.Info.firstName).get &
        Film.Cast.lastName ~> personInfo.get(Person.Info.lastName).get &
        Film.Cast.roleName ~> roleName &
        Film.Cast.personId ~> personId
      ).build()
      Dactor.dactorSelection(context.system, classOf[Film], filmId) ! DefaultMessagingProtocol.InsertIntoRelation("film_cast", Seq(newCastRecord))
    }
  }

  def waitingForInsertAck: Receive = {
    case akka.actor.Status.Success => {
      backTo ! akka.actor.Status.Success
      context.stop(self) // because this is our last state
    }
  }

  def commonBehaviour: Receive = {
    case akka.actor.Status.Failure(e) => fail(e)
  }

  private def fail(e: Throwable): Unit = {
    backTo ! akka.actor.Status.Failure(e)
    context.stop(self)
  }
}

// -----------------------------------------------------
// Notes only
// -----------------------------------------------------

// TODO
// THIS IS IT!
// I can handle state transitions, checking if I have further states,
// killing myself and giving final feedback to my creator through adding
// to the user defined `Receive` PartialFunctions using `andThen`
// But that is not aware of what actually triggered the `Receive`, what
// if another message interrupts the expected messages?
// TODO also make this use a builder pattern
class SequentialFunction(states: Seq[Receive]) extends Actor {
  override def receive: Receive = seqReceive(states.head, states.drop(1))

  def seqReceive(current: Receive, remaining: Seq[Receive]): Receive =
    current.andThen((_) => {context.become(seqReceive(remaining.head, remaining.drop(1)))})
}
