package de.up.hpi.informationsystems.adbms.function

import akka.actor._
import akka.event.LoggingAdapter
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.RelationDef
import de.up.hpi.informationsystems.adbms.function.SequentialFunctor.SequentialFunctorDef
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol.{Request, Success}
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, Relation}
import org.scalatest.enablers.Definition
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContextExecutor
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
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  import SequentialFunctorTest._

  override def afterAll(): Unit = shutdown(system)

  implicit val loggingAdapterDefinition: Definition[LoggingAdapter] = _ != null
  implicit val dispatcherDefinition: Definition[ExecutionContextExecutor] = _ != null

  "A SequentialFunction" when {

    implicit val timeout: Timeout = 1 second

    val d1 = Dactor.dactorOf(system, classOf[PartnerDactor], 1)
    val d2 = Dactor.dactorOf(system, classOf[PartnerDactor], 2)
    val d3 = Dactor.dactorOf(system, classOf[PartnerDactor], 3)

    // selections
    val partnerDactor1 = Dactor.dactorSelection(system, classOf[PartnerDactor], 1)
    val partnerDactor2 = Dactor.dactorSelection(system, classOf[PartnerDactor], 2)
    val partnerDactor3 = Dactor.dactorSelection(system, classOf[PartnerDactor], 3)

    "only consisting of start and end" should {

      val testSimpleSeqFunctor = (recipients: Seq[ActorSelection], marker: String) => {
        val probe = TestProbe()
        implicit val sender: ActorRef = probe.ref

        val fut = SequentialFunctor()
          .start( (_: StartMessage.type) => MessageA.Request(marker), recipients)
          .end(identity)

        val functorRef = Dactor.startSequentialFunctor(fut, system)(StartMessage)
        probe.watch(functorRef)
        probe.expectMsg(MessageA.Success(Relation.empty))
      }

      "handle one receiver correctly" in {
        val recipients = Seq(partnerDactor1)
        testSimpleSeqFunctor(recipients, "one receiver")
      }

      "handle different receivers correctly" in {
        val recipients = Seq(partnerDactor1, partnerDactor2, partnerDactor3)
        testSimpleSeqFunctor(recipients, "different receivers")
      }

      "handle the same receiver multiple times correctly" in {
        val recipients = Seq(partnerDactor1, partnerDactor1, partnerDactor1)
        testSimpleSeqFunctor(recipients, "same receiver multiple times")
      }

    }

    "using a functor context" should {
      val probe = TestProbe()
      val startMessage = MessageA.Request("first message")

      def testSeqFunctorWithContext(testFunction: FunctorContext[MessageA.Request] => Unit): Unit = {
        implicit val sender: ActorRef = probe.ref

        val fut = SequentialFunctor()
          .start( (a: MessageA.Request) => a, Seq(partnerDactor1))
          .nextWithContext( (_, functorContext) => {
            testFunction(functorContext)
            MessageB.Request()
          }, Seq(partnerDactor2))
          .endWithContext( (response, functorContext) => {
            testFunction(functorContext)
            response
          })

        Dactor.startSequentialFunctor(fut, system)(startMessage)
        probe.expectMsg(MessageB.Success(Relation.empty))
      }

      "provide access to the first message received" in {
        testSeqFunctorWithContext(functorContext => {
          functorContext.startMessage shouldEqual startMessage
        })
      }

      "allow access to the functor's logging adapter" in {
        testSeqFunctorWithContext(functorContext => {
          // just log message
          functorContext.log.error("XXXX")
        })
      }

      "support defined proportion of ActorContext behavior" in {
        testSeqFunctorWithContext(functorContext => {
          val subjectProbeRef = TestProbe().ref

          functorContext.self.path shouldEqual system.asInstanceOf[ExtendedActorSystem].guardian.path / "$f"
          functorContext.children shouldEqual Iterable.empty[ActorRef]
          functorContext.dispatcher shouldBe defined
          functorContext.system shouldEqual system
          functorContext.parent shouldEqual system.asInstanceOf[ExtendedActorSystem].guardian

          functorContext.child("") shouldEqual None
          functorContext.watch(subjectProbeRef) shouldEqual subjectProbeRef
          assertThrows[IllegalStateException](functorContext.watchWith(subjectProbeRef, "msg"))
          functorContext.unwatch(subjectProbeRef) shouldEqual subjectProbeRef
        })
      }

      "contain correct senders" in {
        val probe = TestProbe()

        val fut = SequentialFunctor()
          .start((a: MessageA.Request) => a, Seq(partnerDactor1))
          .nextWithContext((_, functorContext) => {
            functorContext.senders should contain only d1
            MessageB.Request()
          }, Seq(partnerDactor2))
          .nextWithContext((_, functorContext) => {
            functorContext.senders should contain only d2
            MessageB.Request()
          }, Seq(partnerDactor1, partnerDactor2, partnerDactor3))
          .endWithContext((response, functorContext) => {
            functorContext.senders should contain only (d1, d2, d3)
            response
          })

        implicit val sender: ActorRef = probe.ref
        Dactor.startSequentialFunctor(fut, system)(startMessage)
        probe.expectMsg(MessageB.Success(Relation.empty))
      }
    }

    "having multiple steps" should {

      val fut = SequentialFunctor()
        .start( (_: StartMessage.type) => MessageA.Request("multistep functor: message 1"), Seq(partnerDactor1))
        .next( _ => MessageA.Request("multistep functor: message 2"), Seq(partnerDactor2))
        .next( _ => MessageA.Request("multistep functor: message 3"), Seq(partnerDactor3))
        .end( response => MessageB.Success(response.result))

      "successfully return the final result" in {
        val probe = TestProbe()
        implicit val sender: ActorRef = probe.ref

        val functorRef = Dactor.startSequentialFunctor(fut, system)(StartMessage)
        probe.watch(functorRef)
        probe.expectMsg(MessageB.Success(Relation.empty))
      }

    }

    "wrapped in another actor should return a successful result" in {
      class AUT() extends Actor with ActorLogging {

        private val recipients = Seq(
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

        override def receive: Receive = begin

        private def begin: Receive = {
          case m: MessageA.Request =>
            log.info(s"setting receiver to $sender and starting functor")
            val receiver = sender()
            context.watch(Dactor.startSequentialFunctor(mySequentialFunction, system)(m))
            context.become(end(receiver))

          case m =>
            log.error(s"received unexpected message: $m")
        }

        private def end(receiver: ActorRef): Receive = {
          case m: MessageB.Success =>
            log.info(s"seq fun done")
            receiver ! m

          case Terminated(ref: ActorRef) =>
            log.info(s"functor $ref was terminated")

          case m =>
            log.error(s"received unexpected message: $m")
        }
      }

      val probe = TestProbe()
      implicit val sender: ActorRef = probe.ref
      val ref = system.actorOf(Props(new AUT), "AUT")
      ref ! MessageA.Request("AUT test message")
      probe.expectMsgType[MessageB.Success]
    }
  }

}
