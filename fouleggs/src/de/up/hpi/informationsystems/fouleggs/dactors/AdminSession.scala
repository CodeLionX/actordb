package de.up.hpi.informationsystems.fouleggs.dactors

import akka.actor.{Actor, Props}

object AdminSession {

  final case class Up()

  // :addFilm(filmName, filmDescription, filmRelease)
  // :addPerson(firstName, lastName birthDay)

  // Core functionality for MultiDactorFunction impl:
  // :addCastToFilm(personId, filmId, roleName) <--- MultiDactorFunction! // Range query on non-optimally layed out data: // :findPerson(freeText: firstName andor lastName andor birthday)

  // Only for consideration:
  // :find(freeText: Person.firstName or Film.Info.title or Film.Cast.roleName etc)

  def props: Props = Props[AdminSession]
}

/**
  * Provides top level functionalities
  */
class AdminSession extends Actor {
  override def receive: Receive = commonBehaviour

  def commonBehaviour: Receive = {
    case AdminSession.Up => sender() ! akka.actor.Status.Success
  }
}
