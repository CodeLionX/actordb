package de.up.hpi.informationsystems.adbms.protocols

import de.up.hpi.informationsystems.adbms.definition.Record

object DefaultMessagingProtocol {

  case class InsertIntoRelation(relation: String, records: Seq[Record])

}
