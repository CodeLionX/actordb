package de.up.hpi.informationsystems.adbms

import akka.actor.{Actor, ActorLogging}

import de.up.hpi.informationsystems.adbms.definition.Relation

abstract class Dactor(name: String) extends Actor with ActorLogging {

  /**
    * Returns all relations of this actor mapped with their name.
    * @return map of relation name and relation store
    */
  protected val relations: Map[String, Relation]

  override def preStart(): Unit = log.info(s"${this.getClass.getSimpleName}($name) started")
  
  override def postStop(): Unit = log.info(s"${this.getClass.getSimpleName}($name) stopped")
}
