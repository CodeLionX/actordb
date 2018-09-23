package de.up.hpi.informationsystems.adbms.function

import akka.actor.{Actor, ActorRef, ActorSelection, Props}
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol.{Message, Request, Success}

import scala.reflect.ClassTag


object SequentialFunction {

  /** Convenience type alias for a sequential function step tuple consisting of mapping function and receiver actor refs.
    */
  private sealed trait Step[M1, M2] {
    def mapping: M1 => M2
    def recipients: Seq[ActorSelection]
  }

  /** Encapsulates the start step of a sequential function consisting of the mapping from a request to another
    * request and a list of recipients.
    *
    * @param mapping    function mapping the start message of type `A` to a new request message
    * @param recipients list of recipients' actor references
    * @tparam A         type of the start message
    */
  private case class StartStep[A <: Request[_]: ClassTag](mapping: A => Request[_ <: Message], recipients: Seq[ActorSelection])
    extends Step[A, Request[_ <: Message]]

  private case class IntermediateStep[A <: Message, B <: Message](mapping: Success[A] => Request[B], recipients: Seq[ActorSelection])
    extends Step[Success[A], Request[B]] {

    def toUpperBound: IntermediateStep[Message, Message] = this.asInstanceOf[IntermediateStep[Message, Message]]
  }
  private type IntermediateStepT = IntermediateStep[Message, Message]

  private object EndStep {
    def apply[T1 <: Message, T2 <: Message](mapping: Success[T1] => Success[T2]): EndStep[Success[T2]] =
      new EndStep(mapping.asInstanceOf[Success[Message] => Success[T2]])
  }
  private class EndStep[B <: Success[_]: ClassTag](override val mapping: Success[Message] => B)
    extends Step[Success[Message], B] {
    override def recipients: Seq[ActorSelection] = Seq.empty
  }

  /** Creates a new SequentialFunctionBuilder.
    *
    * This builder requires the definition of a start and end function to return a valid
    * [[de.up.hpi.informationsystems.adbms.function.SequentialFunction.SequentialFunctionDef]] that can be run using a
    * [[de.up.hpi.informationsystems.adbms.Dactor]]'s `startSequentialFunction` method or the `Dactor` companion
    * object's method of the same name. The `SequentialFunctionDef` is started with a `Request` message that can be
    * transformed and send to a list of recipients given as a `Seq` of `ActorSelection`. The function sends the
    * transformed request to all corresponding recipients, collects and aggregates their response contents and forwards
    * a corresponsing `Success` message to the next function. The `end` function defines a final transformation of a received
    * `Success` message before sending it back to the `Actor` which started the function.
    *
    * @example {{{
    * // Sequential function definition in Dactor
    * private val complexQuestionFunction: SequentialFunctionDef[ Request[SomeMessage], Success[OtherMessage] ] =
    *   SequentialFunction()
    *     .start( (req: Success[SomeMessage]) => req, Seq(recipient1) )
    *     .next( (res: Success[SomeMessage]) => {
    *       // do something and create new request
    *       val req: Request[OtherMessage] = YetAnotherMessage.Req(someParam)
    *       req
    *     }, Seq(recipient2, recipient3))
    *     .end( (res: Success[OtherMessage]) => {
    *       // transform response before sending it back to this dactor
    *       val newRes: Success[YetAnotherMessage] = YetAnotherMessage.Success(relation)
    *       newRes
    *     })
    *
    * // Instantiate sequential function to answer request in receive
    * override def receive: Receive = {
    *   case message: ComplexQuestion.Request(_) => {
    *     startSequentialFunction(complexQuestionFunction)(message)
    *     context.become(awaitingComplexQuestionResponse)
    *   }
    * }
    *
    * def awaitingComplexQuestionResponse: Receive = {
    *   case message: YetAnotherMessage.Success =>
    *     // do something with the result
    * }
    * }}}
    *
    * @note   this should always be used with [[de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol]]
    *         subclasses
    * @return a new SequentialFunctionBuilder
    */
  def apply(): SequentialFunctionBuilderBare = new SequentialFunctionBuilderBare()

  /** Builder for [[de.up.hpi.informationsystems.adbms.function.SequentialFunction.SequentialFunctionDef]] which can be
    * instantiated into a [[de.up.hpi.informationsystems.adbms.function.SequentialFunction]].
    *
    * The builder's start, next, and end methods can be used to define the steps for the sequential function.
    */
  class SequentialFunctionBuilderBare {

    def start[A <: Request[_]: ClassTag, L <: Message]
             (start: A => Request[L], recipients: Seq[ActorSelection]): SequentialFunctionBuilderStarted[A, L] =
      new SequentialFunctionBuilderStarted[A, L](StartStep(start, recipients), Seq.empty)
  }

  class SequentialFunctionBuilderStarted[A <: Request[_]: ClassTag, L <: Message]
                                        (start: StartStep[A], steps: Seq[IntermediateStepT]) {

    def next[N <: Message]
            (transform: Success[L] => Request[N], recipients: Seq[ActorSelection]): SequentialFunctionBuilderStarted[A, N] =
      new SequentialFunctionBuilderStarted[A, N](start, steps :+ IntermediateStep(transform, recipients).toUpperBound)

    def end[O <: Message]
           (end: Success[L] => Success[O]): SequentialFunctionDef[A, Success[O]] =
      new SequentialFunctionDef[A, Success[O]](start, steps, EndStep(end))
  }

  class SequentialFunctionDef[A <: Request[_]: ClassTag, O <: Success[_]: ClassTag]
                             (start: StartStep[A], steps: Seq[IntermediateStepT], end: EndStep[O]) {

    def props: Props = Props(new SequentialFunction[A, O](start, steps, end))
  }
}


class SequentialFunction[S <: Request[_]: ClassTag, E <: Success[_]: ClassTag]
                        (start: SequentialFunction.StartStep[S],
                         steps: Seq[SequentialFunction.IntermediateStepT],
                         end: SequentialFunction.EndStep[E]) extends Actor {

  override def receive: Receive = startReceive()

  def startReceive(): Receive = {
    case startMessage: S =>
      val request = start.mapping(startMessage)
      start.recipients foreach { _ ! request }
      val backTo = sender()
      context.become(awaitResponsesReceive(start.recipients.length, Seq.empty, steps, backTo))
  }

  def awaitResponsesReceive(totalResponses: Int,
                            receivedResponses: Seq[Success[Message]],
                            pendingSteps: Seq[SequentialFunction.IntermediateStepT],
                            backTo: ActorRef): Receive = {
    case message: Success[_] =>
      if ((totalResponses - (receivedResponses :+ message).length) > 0) {
        context.become(awaitResponsesReceive(totalResponses, receivedResponses :+ message, pendingSteps, backTo))
      } else {

        val constructor = message.getClass.getConstructors()(0)
        val unionResponse = constructor.newInstance((receivedResponses :+ message).map(_.result).reduce(_ union _))

        pendingSteps.headOption match {
          case None =>
            self ! unionResponse
            context.become(endReceive(backTo))
          case Some(nextStep) =>
            self ! unionResponse
            context.become(nextReceive(nextStep.mapping, nextStep.recipients, steps.drop(1), backTo))
        }
      }
  }

  def nextReceive(currentFunction: Success[Message] => Request[Message],
                  currentRecipients: Seq[ActorSelection],
                  pendingSteps: Seq[SequentialFunction.IntermediateStepT],
                  backTo: ActorRef): Receive = {
    case message: Success[Message] =>
      val request = currentFunction(message)
      currentRecipients foreach { _ ! request }
      context.become(awaitResponsesReceive(currentRecipients.length, Seq.empty, pendingSteps, backTo))
  }

  def endReceive(backTo: ActorRef): Receive = {
    case message: Success[Message] =>
      backTo ! end.mapping(message)
      context.stop(self)
  }
}

