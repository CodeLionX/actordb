package de.up.hpi.informationsystems.fouleggs.dactors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Props}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.function.SequentialFunctor
import de.up.hpi.informationsystems.adbms.protocols.DefaultMessagingProtocol.SelectAllFromRelation
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation.Relation
import de.up.hpi.informationsystems.fouleggs.dactors.AdminSession.AddCastToFilm

object AdminSession {

  final case object Up

  object AddCastToFilm {
    sealed trait AddCastToFilm extends RequestResponseProtocol.Message
    final case class Request(personId: Int, filmId: Int, roleName: String) extends RequestResponseProtocol.Request[AddCastToFilm]
    final case class Success(result: Relation) extends RequestResponseProtocol.Success[AddCastToFilm]
    final case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[AddCastToFilm]
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
    case request: AdminSession.AddCastToFilm.Request =>
      addCastToFilm(request)
    case SelectAllFromRelation.Success(rel) => log.info(rel.toString)
  }

  def addCastToFilm(request: AdminSession.AddCastToFilm.Request): Unit = {
    log.info(s"Adding person ${request.personId} as ${request.roleName} to film ${request.filmId}")
    val functor: ActorRef = context.actorOf(CastAndFilmographyFunctor.props(self))
    functor ! request
    context.become(waitingForSuccess(functor) orElse commonBehaviour)
  }

  def waitingForSuccess(from: ActorRef): Receive = {
    case akka.actor.Status.Success if sender == from =>
      context.become(commonBehaviour)
      log.info("Connected cast to film")

      val empire = Dactor.dactorSelection(context, classOf[Film], 1)
      val mark = Dactor.dactorSelection(context, classOf[Person], 1)

      empire ! SelectAllFromRelation.Request(Film.Cast.name)
      mark ! SelectAllFromRelation.Request(Person.Filmography.name)
  }
}

object CastAndFilmographyFunctor {
  def props(backTo: ActorRef): Props =
    Props(new CastAndFilmographyFunctor(backTo))
}

class CastAndFilmographyFunctor(backTo: ActorRef) extends Actor {

  private def addFilmToPersons(film: ActorSelection, person: ActorSelection) = SequentialFunctor()
    .start((_: AdminSession.AddCastToFilm.Request) => Film.GetFilmInfo.Request(), Seq(film))
    .nextWithContext( (message, startMessage) => {
      message.result.records.toOption.flatMap(_.headOption) match {
        case Some(filmInfo: Record) =>
          Person.AddFilmToFilmography.Request(
            startMessage.filmId, filmInfo(Film.Info.title), filmInfo(Film.Info.release),
            startMessage.roleName
          )
      }
    }, Seq(person))
    .endIdentity

  private def addCastToFilm(person: ActorSelection, film: ActorSelection) = SequentialFunctor()
    .start((_: AdminSession.AddCastToFilm.Request) => Person.GetPersonInfo.Request(), Seq(person))
    .nextWithContext( (message, startMessage) => {
      message.result.records.toOption.flatMap(_.headOption) match {
        case Some(personInfo: Record) =>
          Film.AddCast.Request(
            startMessage.personId, personInfo(Person.Info.firstName), personInfo(Person.Info.lastName),
            startMessage.roleName
          )
      }
    }, Seq(film))
    .endIdentity

  private def fail(e: Throwable): Unit = {
    backTo ! akka.actor.Status.Failure(e)
    context.stop(self)
  }

  override def receive: Receive = start()

  def start(): Receive = {
    case request: AdminSession.AddCastToFilm.Request =>
      val personSelection: ActorSelection = Dactor.dactorSelection(context, classOf[Person], request.personId)
      val filmSelection: ActorSelection = Dactor.dactorSelection(context, classOf[Film], request.filmId)

      val sub1 = Dactor.startSequentialFunctor(
        addFilmToPersons(filmSelection, personSelection), context)(
        AddCastToFilm.Request(request.personId, request.filmId, request.roleName)
      )
      val sub2 = Dactor.startSequentialFunctor(
        addCastToFilm(personSelection, filmSelection), context)(
        AddCastToFilm.Request(request.personId, request.filmId, request.roleName)
      )

      context.become(waitingForAck(Seq(sub1, sub2)))
  }

  def waitingForAck(pending: Seq[ActorRef]): Receive = {
    case _: RequestResponseProtocol.Success[_] =>
      val remainingACKs = pending.filterNot(_ == sender())

      if(remainingACKs.isEmpty) {
        backTo ! akka.actor.Status.Success
        context.stop(self)
      } else {
        context.become(waitingForAck(remainingACKs))
      }

    case msg: RequestResponseProtocol.Failure[_] =>
      backTo ! akka.actor.Status.Failure(msg.e)
      context.stop(self)
  }
}
