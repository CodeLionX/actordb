package de.up.hpi.informationsystems.adbms

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, ActorSelection, Props}
import de.up.hpi.informationsystems.adbms.definition.Relation

object Dactor {

  /**
    * Creates a new Dactor of type `clazz` with id `id` in context of the supplied `ActorRefFactory`
    * and returns its ActorRef.
    * @param factory `ActorRefFactory` to be used to create the new Dactor
    * @param clazz class of the Dactor to be created
    * @param id id of the new Dactor
    * @return ActorRef of the newly created Dactor
    */
  def dactorOf(factory: ActorRefFactory, clazz: Class[_ <: Dactor], id: Int): ActorRef =
    factory.actorOf(Props(clazz, id), nameOf(clazz, id))

  /**
    * Constructs the name for a Dactor of type `clazz` and with id `id`.
    * It can be used to create a path.
    * @param clazz class of the Dactor to be created
    * @param id id of the new Dactor
    * @return name of the Dactor with the supplied properties
    */
  def nameOf(clazz: Class[_ <: Dactor], id: Int): String = s"${clazz.getSimpleName}-$id"

}

abstract class Dactor(id: Int) extends Actor with ActorLogging {

  /**
    * Returns all relations of this actor mapped with their name.
    * @return map of relation name and relation store
    */
  protected val relations: Map[String, Relation]

  /**
    * Creates a new Dactor of type `clazz` with id `id` in the same context as this Dactor and returns its ActorRef.
    * @param clazz class of the Dactor to be created
    * @param id id of the new Dactor
    * @return ActorRef of the newly created Dactor
    */
  protected def dactorOf(clazz: Class[_ <: Dactor], id: Int): ActorRef =
    Dactor.dactorOf(context.system, clazz, id)

  /**
    * Looks up the path to a Dactor and returns the `ActorSelection`.
    * @param clazz class of the Dactor
    * @param id id of the Dactor
    * @return ActorSelection of the lookup
    */
  protected def dactorSelection(clazz: Class[_ <: Dactor], id: Int): ActorSelection =
    context.system.actorSelection(context.system / Dactor.nameOf(clazz, id))

  override def preStart(): Unit = log.info(s"${this.getClass.getSimpleName}($id) started")

  override def postStop(): Unit = log.info(s"${this.getClass.getSimpleName}($id) stopped")
}
