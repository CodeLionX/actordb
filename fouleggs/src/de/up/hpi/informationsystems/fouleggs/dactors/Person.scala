package de.up.hpi.informationsystems.fouleggs.dactors

import java.time.ZonedDateTime

import akka.actor.Actor
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef, UntypedColumnDef}
import de.up.hpi.informationsystems.adbms.protocols.{DefaultMessageHandling, RequestResponseProtocol}
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, Relation}

object Person {
  // Relation Definitions
  import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._

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
      Dactor.createAsRowRelations(Seq(Person.Info, Person.Filmography))

    override def receive: Receive = Actor.emptyBehavior
  }
}

class Person(id: Int) extends Person.PersonBase(id) with DefaultMessageHandling

