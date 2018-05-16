package de.up.hpi.informationsystems.adbms

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, ActorSelection, Props}
import de.up.hpi.informationsystems.adbms.definition.Relation

object Dactor {

  def refFor(factory: ActorRefFactory, clazz: Class[_], id: Int): ActorRef =
    factory.actorOf(Props(clazz, s"$id"), nameFor(clazz, s"$id"))

  def nameFor(clazz: Class[_], name: String): String = s"${clazz.getSimpleName}-$name"

}

abstract class Dactor(name: String) extends Actor with ActorLogging {

  /**
    * Returns all relations of this actor mapped with their name.
    * @return map of relation name and relation store
    */
  protected val relations: Map[String, Relation]

  override def preStart(): Unit = log.info(s"${this.getClass.getSimpleName}($name) started")

  override def postStop(): Unit = log.info(s"${this.getClass.getSimpleName}($name) stopped")

  protected def dactorOf(clazz: Class[_], id: Int): ActorRef =
    Dactor.refFor(context.system, clazz, id)

  protected def dactorSelection(clazz: Class[_], id: Int): ActorSelection =
    context.system.actorSelection(context.system / Dactor.nameFor(clazz, s"$id"))
}
