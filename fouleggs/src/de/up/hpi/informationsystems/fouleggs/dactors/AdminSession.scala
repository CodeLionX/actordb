package de.up.hpi.informationsystems.fouleggs.dactors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Props}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.function.SequentialFunction
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
    case AdminSession.AddCastToFilm.Request(personId, filmId, roleName) =>
      addCastToFilm(personId, filmId, roleName)
    case SelectAllFromRelation.Success(rel) => log.info(rel.toString)
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

      empire ! SelectAllFromRelation.Request(Film.Cast.name)
      mark ! SelectAllFromRelation.Request(Person.Filmography.name)
    }
  }
}

object CastAndFilmographyFunctor {
  def props(personId: Int, filmId: Int, roleName: String, backTo: ActorRef): Props =
    Props(new CastAndFilmographyFunctor(personId, filmId, roleName, backTo))
}

class CastAndFilmographyFunctor(personId: Int, filmId: Int, roleName: String, backTo: ActorRef) extends Actor {

  val personSelection: ActorSelection = Dactor.dactorSelection(context.system, classOf[Person], personId)
  val filmSelection: ActorSelection = Dactor.dactorSelection(context.system, classOf[Film], filmId)

  private val addFilmToPersons = SequentialFunction()
    .start((_: AdminSession.AddCastToFilm.Request) => Film.GetFilmInfo.Request(), Seq(filmSelection))
    .next(message => {
      val filmInfoOption: Option[Record] = message.result.records.toOption match {
        case Some(records: Seq[Record]) => records.headOption
        case _ => None
      }
      filmInfoOption match {
        case Some(filmInfo: Record) =>
          Person.AddFilmToFilmography.Request(filmId, filmInfo(Film.Info.title), filmInfo(Film.Info.release), roleName)
      }
    }, Seq(personSelection))
    .end(identity)

  private val addCastToFilm = SequentialFunction()
    .start((_: AdminSession.AddCastToFilm.Request) => Person.GetPersonInfo.Request(), Seq(personSelection))
    .next(message => {
      val personInfoOption: Option[Record] = message.result.records.toOption match {
        case Some(records: Seq[Record]) => records.headOption
        case _ => None
      }
      personInfoOption match {
        case Some(personInfo: Record) =>
          Film.AddCast.Request(personId, personInfo(Person.Info.firstName), personInfo(Person.Info.lastName), roleName)
      }
    }, Seq(filmSelection))
    .end(identity)

  private def fail(e: Throwable): Unit = {
    backTo ! akka.actor.Status.Failure(e)
    context.stop(self)
  }

  private val sub1 = Dactor.startSequentialFunction(addFilmToPersons, context, self)(AddCastToFilm.Request(personId, filmId, roleName))
  private val sub2 = Dactor.startSequentialFunction(addCastToFilm, context, self)(AddCastToFilm.Request(personId, filmId, roleName))

  override def receive: Receive = waitingForAck(Seq(sub1, sub2))

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
