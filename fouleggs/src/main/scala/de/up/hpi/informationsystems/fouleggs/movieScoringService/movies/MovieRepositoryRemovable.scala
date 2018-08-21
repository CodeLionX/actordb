package de.up.hpi.informationsystems.fouleggs.movieScoringService.movies

import akka.actor.{ActorRef, Cancellable, PoisonPill, Terminated}

import scala.concurrent.duration.FiniteDuration

trait MovieRepositoryRemovable { that: MovieRepository =>
  import MovieRepository._

  def handleRemoveById(movies: Map[Int, ActorRef], maxId: Int, timeoutDuration: FiniteDuration): Receive = {
    case RemoveById.Request(id) =>
      log.info(s"Removing movie actor with id $id")
      movies.get(id) match {
        case None =>
          log.warning(s"tried to remove movie actor with id $id that was not existent")
          sender() ! RemoveById.Success

        case Some(movieRef) =>
          movieRef ! PoisonPill

          import context.dispatcher
          val timeout = context.system.scheduler.scheduleOnce(timeoutDuration, self, Timeout)
          context.become(pendingRemoveAck(movies, maxId, movieRef, id, sender(), timeout))
      }
  }

  def pendingRemoveAck(movies: Map[Int, ActorRef], maxId: Int, from: ActorRef, id: Int, ackTo: ActorRef, timeout: Cancellable): Receive = {
    case Terminated if sender() == from =>
      log.info(s"Movie actor successfully stopped and removed from repository")
      timeout.cancel()
      ackTo ! RemoveById.Success
      context.become(entryPoint(movies.filterKeys(_ == id), maxId))

    case Timeout =>
      log.error(s"PoisonPill message timed-out for movie actor $id")
      timeout.cancel()
      ackTo ! RemoveById.Failure(new RuntimeException("Could not stop actor: Timeout"))
      context.become(entryPoint(movies, maxId))

    // put other messages back at the mailbox beginning
    case e =>
      log.debug("forwarding message during waiting for remove ACK")
      self.tell(e, sender())
  }
}