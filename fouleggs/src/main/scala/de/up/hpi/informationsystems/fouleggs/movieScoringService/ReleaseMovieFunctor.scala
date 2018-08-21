package de.up.hpi.informationsystems.fouleggs.movieScoringService

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import de.up.hpi.informationsystems.adbms.protocols.DefaultMessagingProtocol.InsertIntoRelation
import de.up.hpi.informationsystems.fouleggs.movieScoringService.ReleaseMovieFunctor.MovieFunctorTimeoutException
import de.up.hpi.informationsystems.fouleggs.movieScoringService.movies.Movie
import de.up.hpi.informationsystems.fouleggs.movieScoringService.movies.MovieActor.{Cast, Info, Studio, Trailers}
import de.up.hpi.informationsystems.fouleggs.movieScoringService.movies.MovieRepository.{Add, Timeout}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Success
import scala.language.postfixOps

object ReleaseMovieFunctor {
  case class MovieFunctorTimeoutException(msg: String, e: Throwable = null) extends RuntimeException(msg, e)

  def props(repo: ActorRef, movie: Movie, backTo: ActorRef): Props =
    Props(new ReleaseMovieFunctor(repo, movie, backTo))
}

class ReleaseMovieFunctor(repo: ActorRef, movie: Movie, backTo: ActorRef, timeoutDuration: FiniteDuration = 5 seconds) extends Actor with ActorLogging {
  import MovieScoringService._

  override def preStart(): Unit = {
    log.info(s"Sending creation message to repository before starting ${this.getClass.getSimpleName}")
    repo ! Add.Request(movie.info(Info.title), movie.info(Info.description), movie.info(Info.release))

    val timeout = scheduleTimeoutMessage()
    context.become(pendingMovieCreationAck(timeout))
  }

  override def receive: Receive = {
    case m => log.error(s"THIS SHOULD NOT HAPPEN! Received: unexpected message $m")
  }

  def pendingMovieCreationAck(timeout: Cancellable): Receive = {
    case Add.Success(ref, id) =>
      timeout.cancel()

      log.info(s"Populating studio information of movie $id")
      ref ! InsertIntoRelation(Studio.name, Seq(movie.studio))
      val nextTimeout = scheduleTimeoutMessage()
      context.become(pendingStudioAck(nextTimeout, ref, id))

    case Add.Failure(e) =>
      timeout.cancel()
      log.error(s"Movie creation failed! Forwarding error")
      backTo ! MovieRelease.Failed(e)
      context.stop(self)

    case Timeout =>
      backTo ! MovieRelease.Failed(MovieFunctorTimeoutException("Movie creation timeout!"))
      context.stop(self)
  }

  def pendingStudioAck(timeout: Cancellable, movieActor: ActorRef, id: Int): Receive = {
    case akka.actor.Status.Success =>
      timeout.cancel()
      val nextTimeout = scheduleTimeoutMessage()
      movie.cast.records match {
        case Success(records) if records.nonEmpty =>
          log.info(s"Populating cast of movie $id")
          movieActor ! InsertIntoRelation(Cast.name, records)
          context.become(pendingCastAck(nextTimeout, movieActor, id))

        case _ =>
          log.warning(s"cast of movie $id empty or flawed")
          movie.trailer.records match {
            case Success(records) if records.nonEmpty =>
              log.info(s"Populating trailers of movie $id")
              movieActor ! InsertIntoRelation(Trailers.name, records)
              context.become(pendingTrailerAck(nextTimeout, movieActor, id))

            case _ =>
              log.warning(s"trailers of movie $id empty or flawed")
              backTo ! MovieRelease.Succeeded
          }
      }

    case akka.actor.Status.Failure(e) =>
      timeout.cancel()
      log.error(s"Could not update studio information for movie $id! Forwarding error")
      backTo ! MovieRelease.Failed(e)
      context.stop(self)

    case Timeout =>
      backTo ! MovieRelease.Failed(MovieFunctorTimeoutException("Studio information population timed out!"))
      context.stop(self)
  }

  def pendingCastAck(timeout: Cancellable, movieActor: ActorRef, id: Int): Receive = {
    case akka.actor.Status.Success =>
      timeout.cancel()
      val nextTimeout = scheduleTimeoutMessage()
      movie.trailer.records match {
        case Success(records) if records.nonEmpty =>
          log.info(s"Populating trailers of movie $id")
          movieActor ! InsertIntoRelation(Trailers.name, records)
          context.become(pendingTrailerAck(nextTimeout, movieActor, id))

        case _ =>
          log.warning(s"trailers of movie $id empty or flawed")
          backTo ! MovieRelease.Succeeded
      }

    case akka.actor.Status.Failure(e) =>
      timeout.cancel()
      log.error(s"Could not update cast for movie $id! Forwarding error")
      backTo ! MovieRelease.Failed(e)
      context.stop(self)

    case Timeout =>
      backTo ! MovieRelease.Failed(MovieFunctorTimeoutException("Cast population timed out!"))
      context.stop(self)
  }

  def pendingTrailerAck(timeout: Cancellable, movieActor: ActorRef, id: Int): Receive = {
    case akka.actor.Status.Success =>
      timeout.cancel()
      backTo ! MovieRelease.Succeeded

    case akka.actor.Status.Failure(e) =>
      timeout.cancel()
      log.error(s"Could not update trailers for movie $id! Forwarding error")
      backTo ! MovieRelease.Failed(e)
      context.stop(self)

    case Timeout =>
      log.error(s"")
      backTo ! MovieRelease.Failed(MovieFunctorTimeoutException("Trailer population timed out!"))
      context.stop(self)
  }

  def scheduleTimeoutMessage(): Cancellable = {
    import context.dispatcher
    context.system.scheduler.scheduleOnce(timeoutDuration, this.self, Timeout)
  }
}