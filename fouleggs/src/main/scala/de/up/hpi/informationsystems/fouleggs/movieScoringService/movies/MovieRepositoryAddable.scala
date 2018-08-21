package de.up.hpi.informationsystems.fouleggs.movieScoringService.movies

import akka.actor.{ActorRef, Cancellable}

import scala.concurrent.duration.FiniteDuration

trait MovieRepositoryAddable { that: MovieRepository =>
  import MovieRepository._

  def handleAdd(movies: Map[Int, ActorRef], maxId: Int, timeoutDuration: FiniteDuration): Receive = {
    case Add.Request(title, description, release) =>
      val newId = maxId + 1
      log.info(s"Adding new movie actor with id $newId and setting initial state")
      val m: ActorRef = context.actorOf(MovieActor.props(newId), s"movie-$newId")
      m ! MovieActor.UpdateInfo.Request(title, description, release)

      import context.dispatcher
      val timeout = context.system.scheduler.scheduleOnce(timeoutDuration, self, Timeout)
      context.become(pendingAddAck(movies, maxId, m, newId, sender(), timeout))
  }

  def pendingAddAck(movies: Map[Int, ActorRef], maxId: Int, from: ActorRef, newId: Int, ackTo: ActorRef, timeout: Cancellable): Receive = {
    case MovieActor.UpdateInfo.Success(_) if sender() == from =>
      timeout.cancel()
      log.info(s"Created movie actor with id $newId")
      context.watch(from)
      ackTo ! Add.Success(from, newId)
      context.become(entryPoint(movies + (newId -> from), newId))

    case MovieActor.UpdateInfo.Failure(e) if sender() == from =>
      timeout.cancel()
      log.error(s"Movie actor $newId population failed", e)
      ackTo ! Add.Failure(new RuntimeException("Could not create actor", e))
      context.become(entryPoint(movies, maxId))

    case Timeout =>
      log.error(s"Add message timed-out for movie actor $newId creation")
      ackTo ! Add.Failure(new RuntimeException("Could not create actor: Timeout"))
      context.become(entryPoint(movies, maxId))

    // put other messages back at the mailbox beginning
    case e =>
      log.debug(s"requeue message $e")
      self.tell(e, sender())
  }
}