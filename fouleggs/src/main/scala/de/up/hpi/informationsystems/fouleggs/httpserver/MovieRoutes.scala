package de.up.hpi.informationsystems.fouleggs.httpserver

import akka.pattern.ask
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import de.up.hpi.informationsystems.fouleggs.movieScoringService.MovieScoringService.MovieRelease.MovieRelease
import de.up.hpi.informationsystems.fouleggs.movieScoringService.MovieScoringService.{MovieRelease, ReleaseNewMovie}
import de.up.hpi.informationsystems.fouleggs.movieScoringService.movies.Movie

import scala.concurrent.Future

trait MovieRoutes extends JsonSupport { self: ServerTrait =>

  lazy val movieRoutes =
    pathPrefix("movies") {
      concat(
        pathEnd {
          concat(
            post {
              entity(as[Movie]) { movie =>
                val movieCreated: Future[MovieRelease] =
                  (movieScoringService ? ReleaseNewMovie(movie)).mapTo[MovieRelease]

                onSuccess(movieCreated){
                    case MovieRelease.Succeeded =>
                      complete(StatusCodes.Created, "Movie successfully released.")
                    case MovieRelease.Failed(e) =>
                      complete(StatusCodes.BadRequest, s"Movie release failed: $e")
                  }
                }
              }
          )
        }
      )
    }
}
