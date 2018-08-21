package de.up.hpi.informationsystems.fouleggs.httpserver

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.util.Timeout
import de.up.hpi.informationsystems.fouleggs.movieScoringService.MovieScoringService
import de.up.hpi.informationsystems.fouleggs.movieScoringService.movies.MovieRepository

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.io.StdIn
import scala.language.postfixOps

object Server extends App with ServerTrait with MovieRoutes {

  lazy val routes = movieRoutes

  val bindingFuture = Http().bindAndHandle(routes, "localhost", 8080)

  println(s"Server online at http://localhost:8080/\nPress RETURN to stop.")
  StdIn.readLine() // block until RETURN
  bindingFuture
    .flatMap(_.unbind())
    .onComplete(_ => system.terminate())
}

trait ServerTrait {
  implicit val system: ActorSystem = ActorSystem("fouleggs-http")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val execucationContext: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = Timeout(2 seconds)

  val movieRepository: ActorRef = system.actorOf(MovieRepository.props(2 seconds))
  val movieScoringService: ActorRef = system.actorOf(MovieScoringService.props(movieRepository))

}