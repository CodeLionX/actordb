package de.up.hpi.informationsystems.adbms.function

import java.io.{NotSerializableException, ObjectOutputStream}

import akka.actor.{ActorContext, ActorRef, ActorSystem}
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol.Request

import scala.concurrent.ExecutionContextExecutor

object FunctorContext {

  def fromActorContext[T <: Request[_]](context: ActorContext, startMessage: T): FunctorContext[T] =
    FunctorContextFromActorContext(startMessage, context)

  private case class FunctorContextFromActorContext[T <: Request[_]](startMessage: T, actorContext: ActorContext)
    extends FunctorContext[T] {

    override def self: ActorRef = actorContext.self

    override def sender: ActorRef = actorContext.sender

    override def children: Iterable[ActorRef] = actorContext.children

    override def child(name: String): Option[ActorRef] = actorContext.child(name)

    override implicit def dispatcher: ExecutionContextExecutor = actorContext.dispatcher

    override implicit def system: ActorSystem = actorContext.system

    override def parent: ActorRef = actorContext.parent

    override def watch(subject: ActorRef): ActorRef = actorContext.watch(subject)

    override def watchWith(subject: ActorRef, msg: Any): ActorRef = actorContext.watchWith(subject, msg)

    override def unwatch(subject: ActorRef): ActorRef = actorContext.unwatch(subject)
  }
}

/** The functor context. Exposes contextual information for the functor
  * and the current message to be used in transformation functions.
  *
  * @tparam T type of the start message
  */
trait FunctorContext[T <: Request[_]] {

  /** First message received by the functor, so-called `StartMessage`.
    *
    * @return the first message
    */
  def startMessage: T

  /** The ActorRef representing this functor
    *
    * @see [[akka.actor.ActorContext#self]]
    * @return ActorRef representing this functor
    */
  def self: ActorRef

  /** Returns the sender 'ActorRef' of the recently received message.
    *
    * @see [[akka.actor.ActorContext#sender]]
    * @return the sender 'ActorRef'
    */
  def sender: ActorRef

  /** Returns all supervised children; this method returns a view (i.e. a lazy
    * collection) onto the internal collection of children. Targeted lookups
    * should be using `child` instead for performance reasons.
    *
    * @see [[akka.actor.ActorContext#children]]
    * @return lazy view onto all supervised children
    */
  def children: Iterable[ActorRef]

  /** Get the child with the given name if it exists.
    *
    * @see [[akka.actor.ActorContext#child]]
    * @param name name of the child to lookup
    * @return actor ref to child if found, otherwise `None`
    */
  def child(name: String): Option[ActorRef]

  /** Returns the dispatcher (MessageDispatcher) that is used for this functor.
    * Importing this member will place an implicit ExecutionContext in scope.
    *
    * @see [[akka.actor.ActorContext#dispatcher]]
    * @return dispatcher of this functor
    */
  implicit def dispatcher: ExecutionContextExecutor

  /** The system that the functor belongs to.
    * Importing this member will place an implicit ActorSystem in scope.
    *
    * @see [[akka.actor.ActorContext#system]]
    * @return the actor system
    */
  implicit def system: ActorSystem

  /** Returns the supervising parent ActorRef.
    *
    * @see [[akka.actor.ActorContext#parent]]
    * @return supervising parent actor
    */
  def parent: ActorRef

  /** Registers this functor as a Monitor for the provided ActorRef.
    * This functor will receive a Terminated(subject) message when watched
    * actor is terminated.
    *
    * @see [[akka.actor.ActorContext#watch]]
    * @param subject actor ref to watch
    * @return the provided ActorRef
    */
  def watch(subject: ActorRef): ActorRef

  /** Registers this functor as a Monitor for the provided ActorRef.
    * This functor will receive the specified message when watched
    * actor is terminated.
    *
    * @see [[akka.actor.ActorContext#watchWith]]
    * @return the provided ActorRef
    */
  def watchWith(subject: ActorRef, msg: Any): ActorRef

  /** Unregisters this functor as Monitor for the provided ActorRef.
    *
    * @see [[akka.actor.ActorContext#unwatch]]
    * @return the provided ActorRef
    */
  def unwatch(subject: ActorRef): ActorRef

  /** ActorContexts shouldn't be Serializable
    */
  final protected def writeObject(o: ObjectOutputStream): Unit =
    throw new NotSerializableException("FunctorContext is not serializable!")
}