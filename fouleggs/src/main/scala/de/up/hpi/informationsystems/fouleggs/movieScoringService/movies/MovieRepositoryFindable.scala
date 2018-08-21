package de.up.hpi.informationsystems.fouleggs.movieScoringService.movies

import akka.actor.ActorRef

trait MovieRepositoryFindable { that: MovieRepository =>
  import MovieRepository._

  def handleFind(movies: Map[Int, ActorRef]): Receive = {
    case Find.Request(id) =>
      log.info(s"Looking up movie actor ref for id $id")
      movies.get(id) match {
        case Some(movieRef) =>
          log.info(s"Found movie with id $id")
          sender() ! Find.Success(movieRef)

        case None =>
          log.info(s"Movie with id $id not found")
          sender() ! Find.Failure(new RuntimeException(s"Could not find this movie. Use Add.Request($id, [...]) to create it"))
      }
  }
}