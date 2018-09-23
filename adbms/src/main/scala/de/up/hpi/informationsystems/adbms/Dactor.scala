package de.up.hpi.informationsystems.adbms

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, ActorRefFactory, ActorSelection, ActorSystem, Props}
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.adbms.function.SequentialFunction.SequentialFunctionDef
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol.Request
import de.up.hpi.informationsystems.adbms.relation.{FutureRelation, MutableRelation, RowRelation}

import scala.concurrent.Future

object Dactor {

  /**
    * Creates a new Dactor of type `clazz` with id `id` in context of the supplied `ActorRefFactory`
    * and returns its ActorRef.
    *
    * @param factory `ActorRefFactory` to be used to create the new Dactor
    * @param clazz   class of the Dactor to be created
    * @param id      id of the new Dactor
    * @return ActorRef of the newly created Dactor
    */
  def dactorOf(factory: ActorRefFactory, clazz: Class[_ <: Dactor], id: Int): ActorRef =
    factory.actorOf(Props(clazz, id), nameOf(clazz, id))

  /**
    * Looks up the path to a Dactor and returns the `ActorSelection`.
    *
    * @note lookup is global to the system, i.e. /user/`dactorName`
    * @param clazz class of the Dactor
    * @param id    id of the Dactor
    * @return ActorSelection of the lookup
    */
  def dactorSelection(system: ActorRefFactory, clazz: Class[_ <: Dactor], id: Int): ActorSelection =
    system.actorSelection(s"/user/${nameOf(clazz, id)}")

  /**
    * Constructs the name for a Dactor of type `clazz` and with id `id`.
    * It can be used to create a path.
    *
    * @param clazz class of the Dactor to be created
    * @param id    id of the new Dactor
    * @return name of the Dactor with the supplied properties
    */
  def nameOf(clazz: Class[_ <: Dactor], id: Int): String = s"${clazz.getSimpleName}-$id"

  /** Sends `RequestResponseProtocol.Request`s to (multiple) instances of a `Dactor` subclass and returns a
    * FutureRelation which will complete with the unioned results from the respective
    * `RequestResponseProtocol.Responses` if successful or a failed FutureRelation if at least one of the requests
    * fails.
    *
    * @example{{{
    *           // Request and response definition in MyDactor:
    *           object MyMessage {
    *             case class Request() extends RequestResponseProtocol.Request
    *             case class Success(result: Seq[Record]) extends RequestResponseProtocol.Success
    *             case class Failure(e: Throwable) extends RequestResponseProtocol.Failure
    *           }
    *
    *           // Reacting to requests and sending a response in MyDactor:
    *           override def receive: Receive = {
    *             case MyMessage.Request() =>
    *               someInternalMethod match {
    *                 case Success(results) => sender() ! MyMessage.Success(results)
    *                 case Failure(e) => sender() ! MyMessage.Failure(e)
    *               }
    *           }
    *
    *           // [...]
    *
    *           // Sending a request to a MyDactor:
    *           val requestMap = Map(someMyDactorId -> MyDactor.MyMessage.Request())
    *           val futureResponses: FutureRelation = Dactor
    *             .askDactor(system, classOf[MyDactor], requestMap)
    * }}}
    *
    * @note this should always be used with `RequestResponseProtocol` sub-case-classes.
    * @see[[de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol]]
    *
    * @param system       the ActorSystem of the Dactors to send requests to
    * @param dactorClass  the class of Dactors to send requests to
    * @param messages     a Map from Dactor ids to `RequestResponseProtocol.Request`s to send to the respective Dactors
    * @param timeout      failure timeout waiting for the response
    * @return             a FutureRelation of the unioned results from the Requests on successful completion
    */
  def askDactor(
                system: ActorSystem,
                dactorClass: Class[_ <: Dactor],
                messages: Map[Int, RequestResponseProtocol.Request[_ <: RequestResponseProtocol.Message]]
               )(
                implicit timeout: Timeout
               ): FutureRelation = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val results = messages.keys
      .map(dactorId => {
        val msg = messages(dactorId)
        val answer: Future[Any] = akka.pattern.ask(dactorSelection(system, dactorClass, dactorId), msg)(timeout)
        // FIXME: match on result and handle success / failure differences!!!!
        answer
          .mapTo[RequestResponseProtocol.Response[_]]
          .map{
            case s: RequestResponseProtocol.Success[_] => scala.util.Success(s.result)
            case f: RequestResponseProtocol.Failure[_] => scala.util.Failure(f.e)
          }
          .map(_.get)
      })

    FutureRelation(Future.sequence(results).map(_.reduce( (rel1, rel2) => rel1.union(rel2))))
  }

  /**
    * Constructs a mapping of relation definitions and corresponding relational stores using
    * [[de.up.hpi.informationsystems.adbms.relation.RowRelation]] as base relation.
    *
    * @param relDefs sequence of relation definitions
    * @return mapping of relation definition and corresponding relational row store
    */
  def createAsRowRelations(relDefs: Seq[RelationDef]): Map[RelationDef, MutableRelation] =
    relDefs.map(relDef =>
      relDef -> RowRelation(relDef)
    ).toMap

  /** Starts a new actor instance for the [[SequentialFunctionDef]] (functor) and returns its [[ActorRef]].
    *
    * @param function   function definition
    * @param refFactory factory for actor references, like `context`
    * @param message    start message
    * @param sender     implicit sender reference for the message
    * @tparam S         start message type
    * @return           actor reference to the new functor
    */
  def startSequentialFunction[S <: Request[_]](function: SequentialFunctionDef[S, _], refFactory: ActorRefFactory)
                                              (message: S)
                                              (implicit sender: ActorRef): ActorRef = {
    val ref = refFactory.actorOf(function.props)
    ref.tell(message, sender)
    ref
  }
}

abstract class Dactor(id: Int) extends Actor with ActorLogging {

  /**
    * Returns the name of this Dactor used for lookup.
    *
    * @return name of this Dactor
    */
  protected val name: String = Dactor.nameOf(this.getClass, id)

  /**
    * Returns a map of relation definition and corresponding relational store.
    *
    * @return map of relation definition and corresponding relational store
    */
  protected val relations: Map[RelationDef, MutableRelation]

  /**
    * Returns all relations of this actor mapped with their name.
    *
    * @return map of relation name and relational store
    */
  protected def relationFromName: Map[String, MutableRelation] = relations.map(mapping => {
    mapping._1.name -> mapping._2
  })

  /**
    * Creates a new Dactor of type `clazz` with id `id` in the same context as this Dactor and returns its ActorRef.
    *
    * @param clazz class of the Dactor to be created
    * @param id    id of the new Dactor
    * @return ActorRef of the newly created Dactor
    */
  protected def dactorOf(clazz: Class[_ <: Dactor], id: Int): ActorRef =
    Dactor.dactorOf(context.system, clazz, id)

  /**
    * Looks up the path to a Dactor and returns the `ActorSelection`.
    *
    * @param clazz class of the Dactor
    * @param id    id of the Dactor
    * @return ActorSelection of the lookup
    */
  protected def dactorSelection(clazz: Class[_ <: Dactor], id: Int): ActorSelection =
    Dactor.dactorSelection(context.system, clazz, id)

  override def preStart(): Unit = log.info(s"${this.getClass.getSimpleName}($id) started")

  override def postStop(): Unit = log.info(s"${this.getClass.getSimpleName}($id) stopped")

  // TODO timeout and error handler
  def startSequentialFunction[S <: Request[_]](function: SequentialFunctionDef[S, _])(message: S): ActorRef = {
    Dactor.startSequentialFunction(function, context)(message)(self)
    // TODO watch and execute error handler
  }
}
