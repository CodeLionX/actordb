package de.up.hpi.informationsystems.fouleggs

import java.net.URL
import java.time.ZonedDateTime

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import de.up.hpi.informationsystems.fouleggs.movieScoringService.MovieScoringService
import de.up.hpi.informationsystems.fouleggs.movieScoringService.MovieScoringService.ReleaseNewMovie
import de.up.hpi.informationsystems.fouleggs.movieScoringService.movies.MovieActor.{Cast, Info, Studio, Trailers}
import de.up.hpi.informationsystems.fouleggs.movieScoringService.movies.{Movie, MovieRepository}
import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.relation.Relation

import scala.concurrent.duration._
import scala.language.postfixOps

object DemoClient {

  class Client(service: ActorRef) extends Actor with ActorLogging {
    override def preStart(): Unit = {
      val movie: Movie = {
        val info = Info.newRecord(
          Info.title ~> "Crazy Rich Asians" &
            Info.description ~> "\"Crazy Rich Asians\" follows native New Yorker Rachel Chu (Wu) as she accompanies her longtime boyfriend, Nick Young (Golding), to his best friend's wedding in Singapore. Exicted ..." &
            Info.release ~> ZonedDateTime.parse("2018-08-15T00:00:00+01:00")
        ).build()
        val cast = Relation(Seq(
          Cast.newRecord(
            Cast.firstName ~> "Constance" &
              Cast.lastName ~> "Wu" &
              Cast.roleName ~> "Rachel Chu"
          ).build(),
          Cast.newRecord(
            Cast.firstName ~> "Henry" &
              Cast.lastName ~> "Golding" &
              Cast.roleName ~> "Nick Young"
          ).build(),
          Cast.newRecord(
            Cast.firstName ~> "Gemma" &
              Cast.lastName ~> "Chan" &
              Cast.roleName ~> "Astrid Leong"
          ).build(),
          Cast.newRecord(
            Cast.firstName ~> "Harry" &
              Cast.lastName ~> "Shum Jr." &
              Cast.roleName ~> "Charlie Wu"
          ).build(),
          Cast.newRecord(
            Cast.firstName ~> "" &
              Cast.lastName ~> "Awkwafina" &
              Cast.roleName ~> "Goh Peik Lin"
          ).build(),
          Cast.newRecord(
            Cast.firstName ~> "Sonoya" &
              Cast.lastName ~> "Mizuno" &
              Cast.roleName ~> "Araminta Lee"
          ).build()
        ))
        val trailers = Relation(Seq(
          Trailers.newRecord(
            Trailers.title ~> "Crazy Rich Asians: Teaser Trailer 1" &
              Trailers.videoUrl ~> new URL("https://www.rottentomatoes.com/m/crazy_rich_asians") &
              Trailers.length ~> java.time.Duration.ofSeconds(23)
          ).build(),
          Trailers.newRecord(
            Trailers.title ~> "Crazy Rich Asians: Trailer 1" &
              Trailers.videoUrl ~> new URL("https://www.rottentomatoes.com/m/crazy_rich_asians") &
              Trailers.length ~> java.time.Duration.ofSeconds(164)
          ).build()
        ))
        val studio = Studio.newRecord(
          Studio.studioname ~> "Warner Bros. Pictures" &
            Studio.url ~> new URL("http://www.crazyrichasiansmovie.com/")
        ).build()
        Movie(info, trailers, cast, studio)
      }

      service ! ReleaseNewMovie(movie)
      service ! ReleaseNewMovie(movie)
      service ! ReleaseNewMovie(movie)
      service ! ReleaseNewMovie(movie)
      service ! ReleaseNewMovie(movie)
      service ! ReleaseNewMovie(movie)
      service ! ReleaseNewMovie(movie)
      service ! ReleaseNewMovie(movie)
      service ! ReleaseNewMovie(movie)
      service ! ReleaseNewMovie(movie)
      service ! ReleaseNewMovie(movie)
      service ! ReleaseNewMovie(movie)
    }

    override def receive: Receive = {
      case m => log.info(s"Received message: $m")
    }
  }

  def clientProps(service: ActorRef): Props = Props(new Client(service))

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("demo-client-system")
    val movieRepo = system.actorOf(MovieRepository.props(2 seconds), "movie-repository")
    val service = system.actorOf(MovieScoringService.props(movieRepo), "movie-service")
    system.actorOf(clientProps(service), "client")
  }
}
