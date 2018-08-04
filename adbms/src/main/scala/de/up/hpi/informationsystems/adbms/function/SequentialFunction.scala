package de.up.hpi.informationsystems.adbms.function

import akka.actor.{Actor, ActorRef, ActorSelection, Props}
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol.{Message, Request, Success}

import scala.reflect.ClassTag

object SequentialFunction {

  /** Creates a new SequentialFunctionBuilder.
    *
    * This builder requires the definition of a start and end function to return a valid
    * [[de.up.hpi.informationsystems.adbms.function.SequentialFunction.SequentialFunctionDef]] that can be run using a
    * [[de.up.hpi.informationsystems.adbms.Dactor]]'s `startSequentialFunction` method or the `Dactor` companion
    * object's method of the same name. The `SequentialFunctionDef` is started with a `Request` message that can be
    * transformed and send to a list of recipients given as a `Seq` of `ActorSelection`. The function sends the
    * transformed request to all corresponding recipients, collects and aggregates their response contents and forwards
    * a corresponsing `Success` message to the next function. The `end` function defines a final transform of a received
    * `Success` message before sending it back to the `Actor` which started the function.
    *
    * @example{{{
    *           // Sequential function definition in Dactor
    *           private val complexQuestionFunction: SequentialFunctionDef[ Request[SomeMessage], Success[OtherMessage] ] =
    *             SequentialFunction()
    *               .start((req: Success[SomeMessage]) => req, Seq(recipient1))
    *               .next((res: Success[SomeMessage]) => {
    *                 // do something and create new request
    *                 val req: Request[OtherMessage] = YetAnotherMessage.Req(someParam)
    *                 req
    *               }, Seq(recipient2, 3,)
    *               .end((res: Success[OtherMessage]) => {
    *                 // transform response before sending it back to this dactor
    *                 val newRes: Success[YetAnotherMessage] = YetAnotherMessage.Success(relation)
    *                 newRes
    *               })
    *
    *           // Instantiate sequential function to answer request in receive
    *           override def receive: Receive = {
    *             case message: ComplexQuestion.Request(_) => {
    *               startSequentialFunction(complexQuestionFunction, message)
    *               context.become(awaitingComplexQuestionResponse)
    *             }
    *           }
    *
    *           def awaitingComplexQuestionResponse: Receive = {
    *             case message: YetAnotherMessage.Success =>
    *               // do something with the result
    *           }
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
    * This builder's start, next, and end methods can be used to define
    *
    */
  class SequentialFunctionBuilderBare {
    def start[A <: Request[_]: ClassTag, L <: Message]
             (start: A => Request[L],
              recipients: Seq[ActorSelection]):SequentialFunctionBuilderStarted[A, L] =
      new SequentialFunctionBuilderStarted[A, L]((start, recipients), Seq.empty)
  }

  /**
    *
    * @param start
    * @param steps
    * @param ev$1
    * @tparam A
    * @tparam L
    */
  class SequentialFunctionBuilderStarted[A <: Request[_]: ClassTag, L <: Message]
                                        (start: (A => Request[_ <: Message], Seq[ActorSelection]),
                                         steps: Seq[(Success[Message] => Request[Message], Seq[ActorSelection])]) {
    /**
      *
      * @param transform
      * @param recipients
      * @tparam N
      * @return
      */
    def next[N <: Message](transform: Success[L] => Request[N],
                           recipients: Seq[ActorSelection]): SequentialFunctionBuilderStarted[A, N] =
      new SequentialFunctionBuilderStarted[A, N](start, steps :+ (transform.asInstanceOf[Success[Message] => Request[Message]], recipients))

    /**
      *
      * @param end
      * @tparam O
      * @return
      */
    def end[O <: Message](end: Success[L] => Success[O]): SequentialFunctionDef[A, Success[O]] =
      new SequentialFunctionDef[A, Success[O]](start, steps, end.asInstanceOf[Success[Message] => Success[O]])
  }

  /**
    *
    * @param start
    * @param steps
    * @param end
    * @param ev$1
    * @param ev$2
    * @tparam A
    * @tparam O
    */
  class SequentialFunctionDef[A <: Request[_]: ClassTag, O <: Success[_]: ClassTag]
                                          (start: (A => Request[_ <: Message], Seq[ActorSelection]),
                                           steps: Seq[(Success[Message] => Request[Message], Seq[ActorSelection])],
                                           end: Success[Message] => O) {
    def props: Props = Props(new SequentialFunction[A, O](start, steps, end))
  }
}

/**
  *
  * @param start
  * @param steps
  * @param end
  * @param ev$1
  * @param ev$2
  * @tparam S
  * @tparam E
  */
class SequentialFunction[S <: Request[_]: ClassTag, E <: Success[_]: ClassTag]
                        (start: (S => Request[_ <: Message], Seq[ActorSelection]),
                         steps: Seq[(Success[Message] => Request[Message], Seq[ActorSelection])],
                         end: Success[Message] => E) extends Actor {
  override def receive: Receive = startReceive()

  def startReceive(): Receive = {
    case startMessage: S =>
      val request = start._1(startMessage)
      start._2 foreach { _ ! request }
      val backTo = sender()
      context.become(awaitResponsesReceive(start._2.length, Seq.empty, steps, backTo))
  }

  def awaitResponsesReceive(totalResponses: Int,
                            receivedResponses: Seq[Success[Message]],
                            pendingSteps: Seq[(Success[Message] => Request[Message], Seq[ActorSelection])],
                            backTo: ActorRef): Receive = {
    case message: Success[_] =>
      if ((totalResponses - (receivedResponses :+ message).length) > 0) {
        context.become(awaitResponsesReceive(totalResponses, receivedResponses :+ message, pendingSteps, backTo))
      } else {

        val constructor = message.getClass.getConstructors()(0)
        val unionResponse = constructor.newInstance((receivedResponses :+ message).map(_.result).reduce(_ union _))

        if (pendingSteps.nonEmpty) {
          self ! unionResponse
          context.become(nextReceive(pendingSteps.head._1, pendingSteps.head._2, steps.drop(1), backTo))
        } else {
          self ! unionResponse
          context.become(endReceive(backTo))
        }
      }
  }

  def nextReceive(currentFunction: Success[Message] => Request[Message],
                  currentRecipients: Seq[ActorSelection],
                  pendingSteps: Seq[(Success[Message] => Request[Message], Seq[ActorSelection])],
                  backTo: ActorRef): Receive = {
    case message: Success[Message] =>
      val request = currentFunction(message)
      currentRecipients foreach { _ ! request }
      context.become(awaitResponsesReceive(currentRecipients.length, Seq.empty, pendingSteps, backTo))
  }

  def endReceive(backTo: ActorRef): Receive = {
    case message: Success[Message] =>
      backTo ! end(message)
      context.stop(self)
  }

  def props: Props = Props[SequentialFunction[S, E]]
}

