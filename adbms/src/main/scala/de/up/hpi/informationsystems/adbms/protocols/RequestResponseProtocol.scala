package de.up.hpi.informationsystems.adbms.protocols

import de.up.hpi.informationsystems.adbms.relation.Relation

object RequestResponseProtocol {

  /** Marker trait to be subclassed and used with [[Request]], [[Success]], and [[Failure]] as message type information.
    *
    * Subclasses of this marker trait are used as type parameters for [[Request]], [[Success]], and [[Failure]]
    * subclasses. The marker trait then allows for specific pattern matching on [[Request]] and [[Response]] subtypes.
    *
    * @example{{{
    *           // Request and response message definitions in some Dactors companion object implementation:
    *           object MyMessage {
    *             sealed trait MyMessage extends Message
    *             case class Request(someParam: SomeType) extends RequestResponseProtocol.Request[MyMessage]
    *             case class Success(result: Relation) extends RequestResponseProtocol.Success[MyMessage]
    *             case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[MyMessage]
    *           }
    *
    *           object OtherMessage {
    *             sealed trait OtherMessage extends Message
    *             case class Request(someParam: SomeType) extends RequestResponseProtocol.Request[OtherMessage]
    *             case class Success(result: Relation) extends RequestResponseProtocol.Success[OtherMessage]
    *             case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[OtherMessage]
    *           }
    *
    *           // Receive function of same Dactor reacting to [[Success]] messages of different types:
    *           override def receive: Receive = {
    *             case message: MyMessage.Success => // do something with this relation
    *             case message: OtherMessage.Success => // do something else with that relation
    *             case message: RequestResponseProtocol.Failure[_] => // match on the superclass to have common behavior
    *               log.error(message.e.getMessage)
    *           }
    * }}}
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
