package de.up.hpi.informationsystems.adbms.protocols

import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation.Relation

import scala.util.Try

/**
  * Mixin this trait to let your `Dactor` automatically handle messages from
  * [[de.up.hpi.informationsystems.adbms.protocols.DefaultMessagingProtocol]].
  *
  * @example
  * {{{
  * class MyDactorBase extends Dactor {
  *   override protected val relations: Map[RelationDef, MutableRelation] = ...
  *   override def receive: Receive = ...
  * }
  * class MyDactor extends MyDactorBase with DefaultMessageHandling
  * }}}
  */
trait DefaultMessageHandling extends Dactor {

  abstract override def receive: Receive = handleRequest orElse super.receive

  private def handleRequest: Receive = {
    case DefaultMessagingProtocol.InsertIntoRelation(relationName, records) =>
      handleGenericInsert(relationName, records) match {
        case util.Success(_) => sender() ! akka.actor.Status.Success
        case util.Failure(e) => sender() ! akka.actor.Status.Failure(e)
      }
    case DefaultMessagingProtocol.SelectAllFromRelation.Request(relationName) =>
      handleGenericRelationQuery(relationName) match {
        case util.Success(relation) => sender() ! DefaultMessagingProtocol.SelectAllFromRelation.Success(relation)
        case util.Failure(e) => sender() ! DefaultMessagingProtocol.SelectAllFromRelation.Failure(e)
      }
  }

  /**
    * Inserts the specified records into the relation and returns the number of successfully inserted records.
    *
    * @param relationName name of the relation the records should be inserted to
    * @param records      records to be inserted
    * @return either number of successfully inserted records or a `Throwable` describing the failure
    */
  private def handleGenericInsert(relationName: String, records: Seq[Record]): Try[Int] =
    if(records.length == 1)
      records.headOption match {
        case Some(record) =>
          Try {
            relationFromName(relationName).insert(record).map( _=> 1)
          }.flatten
        case None => Try(0)
      }
    else
      Try {
        relationFromName(relationName).insertAll(records).map(_.count(_ => true))
      }.flatten

  /**
    * Returns an immutable copy of the requested relation if the `Dactor` has a `Relation` of this name,
    * otherwise returns a [[NoSuchElementException]] Failure.
    *
    * @param relationName
    * @return an immutable copy of the requested `Relation` or a [[NoSuchElementException]]
    */
  private def handleGenericRelationQuery(relationName: String): Try[Relation] = Try{
    relationFromName(relationName).immutable
  }
}
