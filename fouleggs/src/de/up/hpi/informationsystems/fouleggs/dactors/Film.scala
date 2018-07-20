package de.up.hpi.informationsystems.fouleggs.dactors

import java.time.ZonedDateTime

import akka.actor.Actor
import de.up.hpi.informationsystems.adbms.{Dactor, relation}
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef, UntypedColumnDef}
import de.up.hpi.informationsystems.adbms.protocols.DefaultMessageHandling
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, Relation}

object Film {
  // implicit default values
  import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._

  object Info extends RelationDef {
    // val filmId: ColumnDef[Int] = ColumnDef[Int]("film_id") should be same as the Dactors id and therefor superfluous
    val title: ColumnDef[String] = ColumnDef("film_title", "Untitled")
    val description: ColumnDef[String] = ColumnDef("film_descr")
    val release: ColumnDef[ZonedDateTime] = ColumnDef("film_release")

    override val name: String = "film_info"
    override val columns: Set[UntypedColumnDef] = Set(title, description)
  }

  object Cast extends RelationDef {
    val personId: ColumnDef[Int] = ColumnDef[Int]("person_id")
    val firstName: ColumnDef[String] = ColumnDef("first_name")
    val lastName: ColumnDef[String] = ColumnDef("last_name")
    val roleName: ColumnDef[String] = ColumnDef("role_name")

    override val name: String = "film_cast"
    override val columns: Set[UntypedColumnDef] = Set()
  }

  class FilmBase(id: Int) extends Dactor(id) {
    /**
      * Returns a map of relation definition and corresponding relational store.
      *
      * @return map of relation definition and corresponding relational store
      */
    override protected val relations: Map[RelationDef, MutableRelation] =
      Dactor.createAsRowRelations(Seq(Film.Info, Film.Cast))

    override def receive: Receive = Actor.emptyBehavior
  }
}

class Film(id: Int) extends Film.FilmBase(id) with DefaultMessageHandling

