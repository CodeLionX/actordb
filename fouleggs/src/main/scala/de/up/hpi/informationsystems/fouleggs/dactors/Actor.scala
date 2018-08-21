package de.up.hpi.informationsystems.fouleggs.dactors

import java.time.ZonedDateTime

import akka.actor.{Actor => AkkaActor}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef}
import de.up.hpi.informationsystems.adbms.protocols.DefaultMessageHandling
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, RowRelation, SingleRowRelation}

object Actor {
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
  class ActorBase(id: Int) extends Dactor(id) {

    override protected val relations: Map[RelationDef, MutableRelation] =
      Map(Actor.Info -> SingleRowRelation(Actor.Info)) ++
      Map(Actor.Filmography -> RowRelation(Actor.Filmography))

    override def receive: Receive = AkkaActor.emptyBehavior
  }
}

class Actor(id: Int) extends Actor.ActorBase(id) with DefaultMessageHandling

