package de.up.hpi.informationsystems.fouleggs.movieScoringService.movies

import java.time.ZonedDateTime

import akka.actor.{Actor, ActorLogging, ActorRef, Props, SupervisorStrategy}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

object MovieRepository {
  object Find {
    final case class Request(id: Int)
    final case class Success(ref: ActorRef)
    final case class Failure(e: Throwable)
  }

  object Add {
    final case class Request(title: String, description: String, release: ZonedDateTime)
    final case class Success(ref: ActorRef, id: Int)
    final case class Failure(e: Throwable)
  }

  object RemoveById {
    final case class Request(id: Int)
    final case object Success
    final case class Failure(e: Throwable)
  }

  final case object Timeout

  def props(timeout: FiniteDuration): Props = Props(new MovieRepository(timeout))
  def props: Props = Props(new MovieRepository)
}

class MovieRepository(timeout: FiniteDuration = 2 seconds)
  extends Actor
    with ActorLogging
    with MovieRepositoryAddable
    with MovieRepositoryFindable
    with MovieRepositoryRemovable {

  private val timeoutDuration: FiniteDuration = timeout

  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.defaultStrategy

  override def receive: Receive = entryPoint(Map.empty, 0)

  def entryPoint(movies: Map[Int, ActorRef], maxId: Int): Receive =
    handleAdd(movies, maxId, timeoutDuration)
      .orElse(handleRemoveById(movies, maxId, timeoutDuration))
      .orElse(handleFind(movies))
}
