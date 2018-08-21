package de.up.hpi.informationsystems.fouleggs.movieScoringService

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import de.up.hpi.informationsystems.fouleggs.movieScoringService.movies.Movie
import de.up.hpi.informationsystems.fouleggs.movieScoringService.movies.MovieActor.Info

import scala.collection.mutable

object MovieScoringService {
  case class ReleaseNewMovie(movie: Movie)

  object MovieRelease {
    trait MovieRelease
    case object Succeeded extends MovieRelease
    case class Failed(e: Throwable) extends MovieRelease
  }

  def props(movieRepository: ActorRef): Props = Props(new MovieScoringService(movieRepository))
}

class MovieScoringService(movieRepository: ActorRef) extends Actor with ActorLogging {
  import MovieScoringService._

  private val repo = movieRepository
  private val openFunctors: mutable.Map[ActorRef, ActorRef] = mutable.Map.empty

  override def receive: Receive = default

  def default: Receive = {
    case ReleaseNewMovie(movie) =>
      log.info(s"Received request to release a new movie titled ${movie.info.get(Info.title)}")
      val functor = context.actorOf(ReleaseMovieFunctor.props(repo, movie, self))
      context.watch(functor)
      openFunctors.update(functor, sender())

    case msg if openFunctors.contains(sender()) =>
      val functor = sender()
      val originalSender = openFunctors(functor)
      log.info("Forwarding result from functor to client")
      originalSender ! msg
      // reset state
      openFunctors.remove(functor)
      context.unwatch(functor)
      functor ! PoisonPill
  }
}