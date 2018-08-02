package de.up.hpi.informationsystems.adbms.protocols

import de.up.hpi.informationsystems.adbms.relation.Relation

object RequestResponseProtocol {

  /** Message type to be subclassed and used as [[Request]] and [[Response]] type information
    *
    */
  trait Message

  /** A Request to a Dactor for which you expect a Response.
    *
    * Each Dactor that can reply to queries or Functions subclasses these traits with their own message types so one can
    * match on expected message types
    *
    * @tparam T [[RequestResponseProtocol.Message]] subtype defining the message type of this request
    */
  trait Request[+T <: Message]

  /** A Response from a Dactor, sent in response to a [[Request]].
    *
    * Each Dactor that can reply to queries or Functions subclasses these traits with their own message types so one can
    * match on expected message types.
    *
    * @tparam T [[RequestResponseProtocol.Message]] subtype defining the message type of this response
    */
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
