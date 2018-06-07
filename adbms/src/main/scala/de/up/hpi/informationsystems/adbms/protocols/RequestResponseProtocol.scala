package de.up.hpi.informationsystems.adbms.protocols

import de.up.hpi.informationsystems.adbms.definition.Record

object RequestResponseProtocol {

  trait Request

  sealed trait Response

  trait Success extends Response {
    def result: Seq[Record]
  }

  trait Failure extends Response {
    def e: Throwable
  }
}
