package de.up.hpi.informationsystems.fouleggs.movieScoringService.movies

import java.net.URL
import java.time.ZonedDateTime

import akka.actor.Props
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef}
import de.up.hpi.informationsystems.adbms.protocols.{DefaultMessageHandling, RequestResponseProtocol}
import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, Relation, RowRelation, SingleRowRelation}

import scala.util.{Failure, Success}

object MovieActor {
  // implicit default values
  import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._

  // relation definitions
  object Info extends RelationDef {
    // val filmId: ColumnDef[Int] = ColumnDef[Int]("film_id") should be same as the Dactors id and therefor superfluous
    val title: ColumnDef[String] = ColumnDef[String]("film_title", "Untitled")
    val description: ColumnDef[String] = ColumnDef[String]("film_descr")
    val release: ColumnDef[ZonedDateTime] = ColumnDef[ZonedDateTime]("film_release")
    val score: ColumnDef[Double] = ColumnDef[Double]("score")

    override val name: String = "film_info"
    override val columns: Set[UntypedColumnDef] = Set(title, description, release)
  }

  object Trailers extends RelationDef {
    val title: ColumnDef[String] = ColumnDef[String]("trailer_title")
    val videoUrl: ColumnDef[URL] = ColumnDef[URL]("url")
    val length: ColumnDef[java.time.Duration] = ColumnDef[java.time.Duration]("length")

    override val name: String = "trailers"
    override val columns: Set[UntypedColumnDef] = Set(title, videoUrl, length)
  }

  object Cast extends RelationDef {
    val personId: ColumnDef[Int] = ColumnDef[Int]("person_id")
    val firstName: ColumnDef[String] = ColumnDef[String]("first_name")
    val lastName: ColumnDef[String] = ColumnDef[String]("last_name")
    val roleName: ColumnDef[String] = ColumnDef[String]("role_name")

    override val name: String = "film_cast"
    override val columns: Set[UntypedColumnDef] = Set(personId, firstName, lastName, roleName)
  }

  object Studio extends RelationDef {
    val studioname: ColumnDef[String] = ColumnDef[String]("name")
    val url: ColumnDef[URL] = ColumnDef[URL]("url")

    override val name: String = "studio"
    override val columns: Set[UntypedColumnDef] = Set(studioname, url)
  }

  // messages
  object UpdateInfo {
    trait Message extends RequestResponseProtocol.Message
    final case class Request(title: String, description: String, release: ZonedDateTime) extends RequestResponseProtocol.Request[Message]
    final case class Success(result: Relation) extends RequestResponseProtocol.Success[Message]
    final case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[Message]
  }

  // implementation
  class FilmBase(id: Int) extends Dactor(id) {

    override protected val relations: Map[RelationDef, MutableRelation] =
      Map(Info -> SingleRowRelation(Info)) ++
      Map(Trailers -> RowRelation(Trailers)) ++
      Map(Cast -> RowRelation(Cast)) ++
      Map(Studio -> SingleRowRelation(Studio))

    override def receive: Receive = {
      case UpdateInfo.Request(title, description, release) =>
        log.info("updating movie information")
        val result = relations(Info).update(
            Info.title ~> title &
            Info.description ~> description &
            Info.release ~> release
          ).where(Info.title -> { _: String => true })
        result match {
          case Success(_) => sender() ! UpdateInfo.Success(Relation.empty)
          case Failure(e) => sender() ! UpdateInfo.Failure(e)
        }
    }
  }

  def props(id: Int): Props = Props(new MovieActor(id))
}

class MovieActor(id: Int) extends MovieActor.FilmBase(id) with DefaultMessageHandling

