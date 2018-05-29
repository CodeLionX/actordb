package de.up.hpi.informationsystems.adbms.protocols

import de.up.hpi.informationsystems.adbms.definition.Record

object RequestResponseProtocol {

  trait Request

  sealed trait Response

  trait Success extends Response {
    def result: Seq[Record]
  }

  case class Failure(e: Throwable) extends Response
}
