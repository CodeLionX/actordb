package de.up.hpi.informationsystems.adbms.protocols

import de.up.hpi.informationsystems.adbms.relation.Relation

object RequestResponseProtocol {

  /** A Request to a Dactor for which you expect a Response.
    *
    * Each Dactor that can reply to queries subclasses these traits with their own message types so one can match on
    * expected message types
    */
  trait Request

  sealed trait Response

  /** A successful Response from a Dactor in answer to a Request.
    *
    * @see[[de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol.Response]]
    */
  trait Success extends Response {
    def result: Relation
  }

  /** A failure Response that is used to inform about some failure during answering to a Request.
    *
    * @see[[de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol.Response]]
    */
  trait Failure extends Response {
    def e: Throwable
  }
}
