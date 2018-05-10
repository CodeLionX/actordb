package de.up.hpi.informationsystems.adbms

import akka.actor.{Actor, ActorLogging}

import de.up.hpi.informationsystems.adbms.definition.Relation

abstract class Dactor extends Actor with ActorLogging {

  /**
    * Returns all relations of this actor mapped with their name.
    * @return map of relation name and relation store
    */
  protected def relations: Map[String, Relation]

  override def preStart(): Unit = log.info("Supervisor started")

  override def postStop(): Unit = log.info("Supervisor stopped")
}
