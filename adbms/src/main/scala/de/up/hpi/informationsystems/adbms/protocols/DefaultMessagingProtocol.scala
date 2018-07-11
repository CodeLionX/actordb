package de.up.hpi.informationsystems.adbms.protocols

import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation.Relation

/**
  * Provides default messages for the `adbms` framework.
  */
object DefaultMessagingProtocol {

  /**
    * Use this message to directly insert data into the relations of a `Dactor` implementing
    * [[de.up.hpi.informationsystems.adbms.protocols.DefaultMessageHandling]].
    * The `Dactor`s will return with a message from [[akka.actor.Status]].
    *
    * @note Use with caution! This message relies on internal details of `Dactor`s and could lead to tight coupling.
    * @param relation name of the relation in regards
    * @param records to be inserted records
    */
  case class InsertIntoRelation(relation: String, records: Seq[Record])


  object SelectAllFromRelation {
    /**
      * Use this message to request the contents of a relation from a `Dactor` implementing
      * [[de.up.hpi.informationsystems.adbms.protocols.DefaultMessageHandling]].
      * The `Dactor` will return a
      * [[de.up.hpi.informationsystems.adbms.protocols.DefaultMessagingProtocol.SelectAllFromRelation.Success]]
      * message in case of success, or a [[akka.actor.Status.Failure]] in case of failure.
      *
      * @note Use with caution! This message relies on internal details of `Dactor`s and could lead to tight coupling.
      * @param relation name of the requested relation
      */
    case class Request(relation: String)

    /**
      * Message type returned after successful processing of a
      * [[de.up.hpi.informationsystems.adbms.protocols.DefaultMessagingProtocol.SelectAllFromRelation.Request]]
      * by `Dactor`s implementing [[de.up.hpi.informationsystems.adbms.protocols.DefaultMessageHandling]].
      *
      * @param relation immutable copy of the requested relation
      */
    case class Success(relation: Relation)

    /**
      * Message type returned after failed processing of a
      * [[de.up.hpi.informationsystems.adbms.protocols.DefaultMessagingProtocol.SelectAllFromRelation.Request]]
      * by `Dactor`s implementing [[de.up.hpi.informationsystems.adbms.protocols.DefaultMessageHandling]].
      *
      * @param cause throwable failure
      */
    case class Failure(cause: Throwable)
  }
}
