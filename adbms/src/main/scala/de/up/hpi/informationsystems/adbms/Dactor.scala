package de.up.hpi.informationsystems.adbms

import akka.actor.Status.{Failure, Success}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, ActorSelection, ActorSystem, Props}
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.adbms.protocols.{DefaultMessagingProtocol, RequestResponseProtocol}

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.Try

object Dactor {

  /**
    * Creates a new Dactor of type `clazz` with id `id` in context of the supplied `ActorRefFactory`
    * and returns its ActorRef.
    *
    * @param factory `ActorRefFactory` to be used to create the new Dactor
    * @param clazz   class of the Dactor to be created
    * @param id      id of the new Dactor
    * @return ActorRef of the newly created Dactor
    */
  def dactorOf(factory: ActorRefFactory, clazz: Class[_ <: Dactor], id: Int): ActorRef =
    factory.actorOf(Props(clazz, id), nameOf(clazz, id))

  /**
    * Looks up the path to a Dactor and returns the `ActorSelection`.
    *
    * @note lookup is global to the system, i.e. /user/`dactorName`
    * @param clazz class of the Dactor
    * @param id    id of the Dactor
    * @return ActorSelection of the lookup
    */
  def dactorSelection(system: ActorRefFactory, clazz: Class[_ <: Dactor], id: Int): ActorSelection =
    system.actorSelection(s"/user/${nameOf(clazz, id)}")

  /**
    * Constructs the name for a Dactor of type `clazz` and with id `id`.
    * It can be used to create a path.
    *
    * @param clazz class of the Dactor to be created
    * @param id    id of the new Dactor
    * @return name of the Dactor with the supplied properties
    */
  def nameOf(clazz: Class[_ <: Dactor], id: Int): String = s"${clazz.getSimpleName}-$id"

  def askDactor(
                system: ActorSystem, dactorClass: Class[_ <: Dactor], messages: Map[Int, RequestResponseProtocol.Request]
               )(
                implicit timeout: Timeout
               ): FutureRelation = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val results = messages.keys
      .map(dactorId => {
        val msg = messages(dactorId)
        val answer: Future[Any] = akka.pattern.ask(dactorSelection(system, dactorClass, dactorId), msg)(timeout)
        answer
          .mapTo[RequestResponseProtocol.Success]
          .map(_.result)
      })

    FutureRelation.fromRecordSeq(Future.sequence(results).map(_.flatten.toSeq))
  }

  /**
    * Constructs a mapping of relation definitions and corresponding relational stores using
    * [[de.up.hpi.informationsystems.adbms.definition.RowRelation]] als base relation.
    *
    * @param relDefs sequence of relation definitions
    * @return mapping of relation definition and corresponding relational row store
    */
  def createAsRowRelations(relDefs: Seq[RelationDef]): Map[RelationDef, MutableRelation] =
    relDefs.map(relDef =>
      relDef -> RowRelation(relDef)
    ).toMap

}

abstract class Dactor(id: Int) extends Actor with ActorLogging {

  /**
    * Returns a map of relation definition and corresponding relational store.
    *
    * @return map of relation definition and corresponding relational store
    */
  protected val relations: Map[RelationDef, MutableRelation]

  /**
    * Returns all relations of this actor mapped with their name.
    *
    * @return map of relation name and relational store
    */
  protected def relationFromName: Map[String, MutableRelation] = relations.map(mapping => {
    mapping._1.name -> mapping._2
  })

  /**
    * Creates a new Dactor of type `clazz` with id `id` in the same context as this Dactor and returns its ActorRef.
    *
    * @param clazz class of the Dactor to be created
    * @param id    id of the new Dactor
    * @return ActorRef of the newly created Dactor
    */
  protected def dactorOf(clazz: Class[_ <: Dactor], id: Int): ActorRef =
    Dactor.dactorOf(context.system, clazz, id)

  /**
    * Looks up the path to a Dactor and returns the `ActorSelection`.
    *
    * @param clazz class of the Dactor
    * @param id    id of the Dactor
    * @return ActorSelection of the lookup
    */
  protected def dactorSelection(clazz: Class[_ <: Dactor], id: Int): ActorSelection =
    Dactor.dactorSelection(context.system, clazz, id)

  override def preStart(): Unit = log.info(s"${this.getClass.getSimpleName}($id) started")

  override def postStop(): Unit = log.info(s"${this.getClass.getSimpleName}($id) stopped")

  override def unhandled(message: Any): Unit = message match {
    case DefaultMessagingProtocol.InsertIntoRelation(relationName, records) =>
      handleGenericInsert(relationName, records) match {
        case util.Success(_) => sender() ! Success
        case util.Failure(e) => sender() ! Failure(e)
      }
    case _ => super.unhandled(message)
  }


  /**
    * Inserts the specified records into the relation and returns the number of successfully inserted records.
    *
    * @param relationName name of the relation the records should be inserted to
    * @param records      records to be inserted
    * @return either number of successfully inserted records or a `Throwable` describing the failure
    */
  private def handleGenericInsert(relationName: String, records: Seq[Record]): Try[Int] = Try {
    relationFromName(relationName).insertAll(records).map(_.count(_ => true))
  }.flatten
}
