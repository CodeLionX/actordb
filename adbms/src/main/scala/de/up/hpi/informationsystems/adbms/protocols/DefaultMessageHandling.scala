package de.up.hpi.informationsystems.adbms.protocols

import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.record.Record

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
