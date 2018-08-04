package de.up.hpi.informationsystems.adbms.function

import akka.actor.{Actor, ActorLogging, ActorRef, Terminated}
import de.up.hpi.informationsystems.adbms.function.SequentialFunction.SequentialFunctionDef
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol.{Request, Response, Success}
import de.up.hpi.informationsystems.adbms.relation.Relation
import org.scalatest.{Matchers, WordSpec}

class SequentialFunctionTest extends WordSpec with Matchers{

  "A SequentialFunction" when {

    object MessageA {
      sealed trait MessageA extends RequestResponseProtocol.Message

      case class Request(variable: Int) extends RequestResponseProtocol.Request[MessageA]
      case class Success(result: Relation) extends RequestResponseProtocol.Success[MessageA]
      case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[MessageA]
    }

    object MessageB {
      sealed trait MessageB extends RequestResponseProtocol.Message

      case class Request() extends RequestResponseProtocol.Request[MessageB]
      case class Success(result: Relation) extends RequestResponseProtocol.Success[MessageB]
      case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[MessageB]
    }

    class AUT() extends Actor with ActorLogging {

      override def receive: Receive = {
        case m: MessageA.Success => println(s"fuck ${m.result}")
        case m: MessageB.Success => println(s"Seq fun done: ${m.result}")
        case m: RequestResponseProtocol.Failure[_] => log.error(m.e.getMessage)

        case m: MessageA.Request =>
          println(s"received message ${m.variable}")
          context.watch(startSequentialFunction(mySequentialFunction)(m))
        case Terminated(ref: ActorRef) => println(s"Function actor $ref was terminated.")
      }

      val recipients = Seq(
        context.actorSelection("/user/testpartner1"),
        context.actorSelection("/user/testpartner2"),
        context.actorSelection("/user/testpartner2"),
        context.actorSelection("/user/testpartner2"),
        context.actorSelection("/user/testpartner2")
      )

      // otherSeqFunction: Op1.Res
      private val mySequentialFunction: SequentialFunctionDef[Request[MessageA.MessageA], Success[MessageB.MessageB]] =
        SequentialFunction()
          .start((a: Request[MessageA.MessageA]) => {
            println("start step")
            a
          }, recipients)
          .next(a => {
            println("next step")
            MessageB.Request()
          }, recipients)
          .end(a => {
            println("end step")
            a
          })

      private val sf = SequentialFunction()

      def startSequentialFunction[S <: Request[_]](function: SequentialFunctionDef[S, _])(message: S): ActorRef = {
        val ref = context.system.actorOf(function.props)
        ref ! message
        ref
      }
    }

  }

}
