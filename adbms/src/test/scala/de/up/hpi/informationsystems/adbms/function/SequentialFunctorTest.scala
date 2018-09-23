package de.up.hpi.informationsystems.adbms.function

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Terminated}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.RelationDef
import de.up.hpi.informationsystems.adbms.function.SequentialFunctor.SequentialFunctorDef
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol.{Request, Response, Success}
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, Relation}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec, WordSpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps

object SequentialFunctorTest {

  object MessageA {
    sealed trait MessageA extends RequestResponseProtocol.Message

    case class Request(variable: String) extends RequestResponseProtocol.Request[MessageA]
    case class Success(result: Relation) extends RequestResponseProtocol.Success[MessageA]
    case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[MessageA]
  }

  object MessageB {
    sealed trait MessageB extends RequestResponseProtocol.Message

    case class Request() extends RequestResponseProtocol.Request[MessageB]
    case class Success(result: Relation) extends RequestResponseProtocol.Success[MessageB]
    case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[MessageB]
  }

  sealed trait StartMessageType extends RequestResponseProtocol.Message
  case object StartMessage extends RequestResponseProtocol.Request[StartMessageType]

  class PartnerDactor(id: Int) extends Dactor(id) with ActorLogging {
    override protected val relations: Map[RelationDef, MutableRelation] = Map.empty

    override def receive: Receive = {
      case m: MessageA.Request =>
        log.info(s"Received message A request: ${m.variable}")
        sender() ! MessageA.Success(Relation.empty)

      case _: MessageB.Request =>
        log.info(s"Received message B request")
        sender() ! MessageB.Success(Relation.empty)

      case m => log.error(s"Received unexpected message: $m")
    }
  }
}

class SequentialFunctorTest extends TestKit(ActorSystem("sequential-functor-test-system"))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  import SequentialFunctorTest._

  override def afterAll(): Unit = shutdown(system)

  "A SequentialFunction" when {

    implicit val timeout: Timeout = 2 second

    val partner1 = Dactor.dactorOf(system, classOf[PartnerDactor], 1)

    "only consisting of start and end" should {

      "handle one partner correctly" in {
        val recipients = Seq(Dactor.dactorSelection(system, classOf[PartnerDactor], 1))

        val fut = SequentialFunctor()
          .start( (_: StartMessage.type) => MessageA.Request("first message"), recipients)
          .end(identity)
        val functorRef = Dactor.startSequentialFunctor(fut, system)(StartMessage)
        watch(functorRef)
        expectMsg(MessageA.Success(Relation.empty))
      }
    }

    "old test" in {
      class AUT() extends Actor with ActorLogging {

        var receiver: ActorRef = Actor.noSender

        override def receive: Receive = {
          case m: MessageA.Success =>
            log.error(s"wrong order! received ${m.getClass}")

          case m: MessageB.Success =>
            log.info("Seq fun done")
            receiver ! m

          case m: RequestResponseProtocol.Failure[_] =>
            log.error(m.e.toString)

          case m: MessageA.Request =>
            if(receiver == Actor.noSender) receiver = sender()
            context.watch(Dactor.startSequentialFunctor(mySequentialFunction, system)(m))

          case Terminated(ref: ActorRef) =>
            log.info(s"Function actor $ref was terminated.")
        }

        val recipients = Seq(
          context.actorSelection("/user/PartnerDactor-2"),
          context.actorSelection("/user/PartnerDactor-3"),
          context.actorSelection("/user/PartnerDactor-3"),
          context.actorSelection("/user/PartnerDactor-3"),
          context.actorSelection("/user/PartnerDactor-3")
        )

        private val mySequentialFunction: SequentialFunctorDef[Request[MessageA.MessageA], Success[MessageB.MessageB]] =
          SequentialFunctor()
            .start[Request[MessageA.MessageA], MessageA.MessageA](identity, recipients)
            .next(_ => MessageB.Request(), recipients)
            .end(identity)
      }

      Dactor.dactorOf(system, classOf[PartnerDactor], 2)
      Dactor.dactorOf(system, classOf[PartnerDactor], 3)

      val ref = system.actorOf(Props(new AUT), "AUT")
      watch(ref)
      ref ! MessageA.Request("AUT test message")
      expectMsgType[MessageB.Success]
    }
  }

}
