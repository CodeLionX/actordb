package de.up.hpi.informationsystems.adbms.protocols

import de.up.hpi.informationsystems.adbms.relation.Relation

object RequestResponseProtocol {

  /**
    *
    */
  trait Message

  /** A Request to a Dactor for which you expect a Response.
    *
    * Each Dactor that can reply to queries subclasses these traits with their own message types so one can match on
    * expected message types
    */
  trait Request[+T <: Message]

  sealed trait Response[+T <: Message]

  /** A successful Response from a Dactor in answer to a Request.
    *
    * @tparam T [[RequestResponseProtocol.Message]] subtype to which this success response belongs
    * @see[[de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol.Response]]
    */
  trait Success[+T <: Message] extends Response[T] {
    def result: Relation
  }

  /** A failure Response that is used to inform about some failure during answering to a Request.
    *
    * @tparam T [[RequestResponseProtocol.Message]] subtype to which this failure response belongs
    * @see[[de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol.Response]]
    */
  trait Failure[+T <: Message] extends Response[T] {
    def e: Throwable
  }
}
