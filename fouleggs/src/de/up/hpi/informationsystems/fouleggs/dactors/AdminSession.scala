package de.up.hpi.informationsystems.fouleggs.dactors

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
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
    case DefaultMessagingProtocol.SelectAllFromRelation.Success(rel) => log.info(rel.toString)
    case s: String => log.info(s"received string $s")
  }

  def addCastToFilm(personId: Int, filmId: Int, roleName: String): Unit = {
    log.info(s"Adding person $personId as $roleName to film $filmId")
    val functor: ActorRef = context.system.actorOf(CastAndFilmographyFunctor.props(personId, filmId, roleName, self))
    context.become(waitingForSuccess(functor) orElse commonBehaviour)
  }

  def waitingForSuccess(from: ActorRef): Receive = {
    case akka.actor.Status.Success if sender == from => {
      context.become(commonBehaviour)
      log.info("Connected cast to film")

      val empire = Dactor.dactorSelection(context.system, classOf[Film], 1)
      val mark = Dactor.dactorSelection(context.system, classOf[Person], 1)

      empire ! DefaultMessagingProtocol.SelectAllFromRelation.Request(Film.Cast.name)
      mark ! DefaultMessagingProtocol.SelectAllFromRelation.Request(Person.Filmography.name)
    }
  }
}

object CastAndFilmographyFunctor {
  def props(personId: Int, filmId: Int, roleName: String, backTo: ActorRef): Props =
    Props(new CastAndFilmographyFunctor(personId, filmId, roleName, backTo))
}

class CastAndFilmographyFunctor(personId: Int, filmId: Int, roleName: String, backTo: ActorRef) extends Actor {

  val sub1: ActorRef = context.system.actorOf(AddFilmFunctor.props(personId, filmId, roleName, self))
  val sub2: ActorRef = context.system.actorOf(AddCastFunctor.props(personId, filmId, roleName, self))

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

object AddFilmFunctor {
  def props(personId: Int, filmId: Int, roleName: String, backTo: ActorRef): Props =
    Props(new AddFilmFunctor(personId: Int, filmId: Int, roleName: String, backTo: ActorRef))
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

object AddCastFunctor {
  def props(personId: Int, filmId: Int, roleName: String, backTo: ActorRef): Props =
    Props(new AddCastFunctor(personId: Int, filmId: Int, roleName: String, backTo: ActorRef))
}

class AddCastFunctor(personId: Int, filmId: Int, roleName: String, backTo: ActorRef) extends Actor {

  override def receive: Receive = waitingForPersonInfo orElse commonBehaviour

  // very first message has to be sent outside of Receives
  Dactor.dactorSelection(context.system, classOf[Person], personId) ! DefaultMessagingProtocol.SelectAllFromRelation.Request("person_info")

  def waitingForPersonInfo: Receive = {
    case DefaultMessagingProtocol.SelectAllFromRelation.Failure(e) => fail(e)
    case DefaultMessagingProtocol.SelectAllFromRelation.Success(relation: Relation) => {
      val personInfo: Record = relation.records.get.head

      val newCastRecord: Record = Film.Cast.newRecord(
        Film.Cast.firstName ~> personInfo.get(Person.Info.firstName).get &
        Film.Cast.lastName ~> personInfo.get(Person.Info.lastName).get &
        Film.Cast.roleName ~> roleName &
        Film.Cast.personId ~> personId
      ).build()

      Dactor.dactorSelection(context.system, classOf[Film], filmId) ! DefaultMessagingProtocol.InsertIntoRelation("film_cast", Seq(newCastRecord))

      context.become(waitingForInsertAck orElse commonBehaviour)
    }
  }

  def waitingForInsertAck: Receive = {
    case akka.actor.Status.Success => {
      backTo ! akka.actor.Status.Success
      context.stop(self)
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
