package de.up.hpi.informationsystems.fouleggs.dactors

import java.time.ZonedDateTime

import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef}
import de.up.hpi.informationsystems.adbms.protocols.{DefaultMessageHandling, RequestResponseProtocol}
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, Relation, RowRelation, SingleRowRelation}
import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.record.Record

import scala.util.{Failure, Success, Try}

object Person {
  // Relation Definitions
  import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._

  object GetPersonInfo {
    sealed trait GetPersonInfo extends RequestResponseProtocol.Message
    final case class Request() extends RequestResponseProtocol.Request[GetPersonInfo]
    final case class Success(result: Relation) extends RequestResponseProtocol.Success[GetPersonInfo]
    final case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[GetPersonInfo]
  }

  object AddFilmToFilmography {
    sealed trait AddFilmToFilmography extends RequestResponseProtocol.Message
    final case class Request(filmId: Int, filmName: String, filmRelease: ZonedDateTime, roleName: String)
      extends RequestResponseProtocol.Request[AddFilmToFilmography]
    final case class Success(result: Relation) extends RequestResponseProtocol.Success[AddFilmToFilmography]
    final case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[AddFilmToFilmography]
  }

  object Info extends RelationDef {
    val personId: ColumnDef[Int] = ColumnDef[Int]("person_id")
    val firstName: ColumnDef[String] = ColumnDef[String]("first_name")
    val lastName: ColumnDef[String] = ColumnDef[String]("last_name")
    val birthday: ColumnDef[ZonedDateTime] = ColumnDef[ZonedDateTime]("birthday")

    override val name: String = "person_info"
    override val columns: Set[UntypedColumnDef] = Set(personId, firstName, lastName, birthday)
  }

  object Filmography extends RelationDef {
    val filmId: ColumnDef[Int] = ColumnDef[Int]("film_id")
    val filmName: ColumnDef[String] = ColumnDef[String]("film_name")
    val filmRelease: ColumnDef[ZonedDateTime] = ColumnDef[ZonedDateTime]("film_release")
    val roleName: ColumnDef[String] = ColumnDef[String]("role_name")

    override val name: String = "filmography"
    override val columns: Set[UntypedColumnDef] = Set(filmId, filmName, filmRelease, roleName)
  }

  // Dactor Definition
  class PersonBase(id: Int) extends Dactor(id) {
    /**
      * Returns a map of relation definition and corresponding relational store.
      *
      * @return map of relation definition and corresponding relational store
      */
    override protected val relations: Map[RelationDef, MutableRelation] =
      Map(Person.Info -> SingleRowRelation(Person.Info)) ++
      Map(Person.Filmography -> RowRelation(Person.Filmography))

    override def receive: Receive = {
      case GetPersonInfo.Request() => sender() ! GetPersonInfo.Success(relations(Info).immutable)
      case AddFilmToFilmography.Request(filmId, filmName, filmRelease, roleName) =>
        handleAddFilmToFilmography(filmId, filmName, filmRelease, roleName) match {
          case Success(record) => sender() ! AddFilmToFilmography.Success(Relation(Seq(record)))
          case Failure(e) => sender() ! AddFilmToFilmography.Failure(e)
        }
    }

    def handleAddFilmToFilmography(filmId: Int, filmName: String, filmRelease: ZonedDateTime, roleName: String): Try[Record] =
      relations(Filmography).insert(Filmography.newRecord(
        Filmography.filmId ~> filmId &
          Filmography.filmName ~> filmName &
          Filmography.filmRelease ~> filmRelease &
          Filmography.roleName ~> roleName
      ).build())
  }
}

class Person(id: Int) extends Person.PersonBase(id) with DefaultMessageHandling

