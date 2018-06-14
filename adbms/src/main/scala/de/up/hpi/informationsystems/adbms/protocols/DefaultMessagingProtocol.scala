package de.up.hpi.informationsystems.adbms.protocols

import de.up.hpi.informationsystems.adbms.definition.Record

/**
  * Provides default messages for the `adbms` framework.
  */
object DefaultMessagingProtocol {

  /**
    * Use this message to directly insert data into the relations of a `Dactor` implementing
    * [[de.up.hpi.informationsystems.adbms.protocols.DefaultMessageHandling]].
    * The `Dactor`s will return with a message from [[akka.actor.Status]].
    * @note Use with caution! This message relies on internal details of `Dactor`s and could lead to tight coupling.
    * @param relation name of the relation in regards
    * @param records to be inserted records
    */
  case class InsertIntoRelation(relation: String, records: Seq[Record])

}
