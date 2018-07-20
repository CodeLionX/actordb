package de.up.hpi.informationsystems.fouleggs.dactors

import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.RelationDef
import de.up.hpi.informationsystems.adbms.protocols.DefaultMessageHandling
import de.up.hpi.informationsystems.adbms.relation.MutableRelation

object AdminSession {
  // :addFilm(filmName, filmDescription, filmRelease)
  // :addPerson(firstName, lastName birthDay)

  // Core functionality for MultiDactorFunction impl:
  // :addCastToFilm(personId, filmId, roleName) <--- MultiDactorFunction!

  // Range query on non-optimally layed out data:
  // :findPerson(freeText: firstName andor lastName andor birthday)

  // Only for consideration:
  // :find(freeText: Person.firstName or Film.Info.title or Film.Cast.roleName etc)

  class AdminSessionBase(id: Int) extends Dactor(id) {
    override protected val relations: Map[RelationDef, MutableRelation] = Map.empty
    override def receive: Receive = ???
  }
}

/**
  * Provides top level functionalities
  * @param id
  */
class AdminSession(id: Int) extends AdminSession.AdminSessionBase(id) with DefaultMessageHandling
