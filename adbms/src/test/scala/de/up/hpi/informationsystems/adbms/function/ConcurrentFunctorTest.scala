package de.up.hpi.informationsystems.adbms.function

import akka.actor._
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.RelationDef
import de.up.hpi.informationsystems.adbms.function.SequentialFunctor.SequentialFunctorDef
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol.{Message, Request, Success}
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, Relation}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

object ConcurrentFunctorTest {

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

class ConcurrentFunctorTest extends TestKit(ActorSystem("sequential-functor-test-system"))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  import ConcurrentFunctorTest._

  override def afterAll(): Unit = shutdown(system)

  "A SequentialFunction" when {

    implicit val timeout: Timeout = 1 second

    Dactor.dactorOf(system, classOf[PartnerDactor], 1)
    Dactor.dactorOf(system, classOf[PartnerDactor], 2)
    Dactor.dactorOf(system, classOf[PartnerDactor], 3)

    // selections
    val partnerDactor1 = Dactor.dactorSelection(system, classOf[PartnerDactor], 1)
    val partnerDactor2 = Dactor.dactorSelection(system, classOf[PartnerDactor], 2)
    val partnerDactor3 = Dactor.dactorSelection(system, classOf[PartnerDactor], 3)

    "only consisting of one message" should {

      object ConcurrentFunctor {
        def apply[S <: Request[_]: ClassTag]
                 (job: S => Request[_ <: Message], recipients: Seq[ActorSelection]): ConcurrentFunctorBuilder[S] =
          new ConcurrentFunctorBuilder[S](Seq((job, recipients)))

        class ConcurrentFunctorBuilder[S <: Request[_]: ClassTag]
                                      (jobs: Seq[(S => Request[_ <: Message], Seq[ActorSelection])]) {
          def and(job: S => Request[_ <: Message], recipients: Seq[ActorSelection]): ConcurrentFunctorBuilder[S] =
            new ConcurrentFunctorBuilder[S](jobs :+ (job, recipients))

          def collect[E <: Success[_]: ClassTag]
                     (end: Success[_ <: Message] => E): ConcurrentFunctorDef[S, E] =
            new ConcurrentFunctorDef[S, E](jobs, end)
        }

        class ConcurrentFunctorDef[S <: Request[_]: ClassTag, E <: Success[_]: ClassTag]
                                  (jobs: Seq[(S => Request[_ <: Message], Seq[ActorSelection])],
                                   collectMapping: Success[Message] => E) {
          def props: Props = Props(new ConcurrentFunctor[S, E](jobs, collectMapping))
        }
      }

      class ConcurrentFunctor[S <: Request[_]: ClassTag, E <: Success[_]: ClassTag]
                             (jobs: Seq[(S => Request[_ <: Message], Seq[ActorSelection])],
                              collectMapping: Success[Message] => E) extends Actor {
        override def receive: Receive = ???
      }

      def startConcurrentFunctor[S <: Request[_]](function: ConcurrentFunctor.ConcurrentFunctorDef[S, _], refFactory: ActorRefFactory)
                                                 (message: S)
                                                 (implicit sender: ActorRef): ActorRef = {
        val ref = refFactory.actorOf(function.props)
        ref.tell(message, sender)
        ref
      }

      val testSimpleSeqFunctor = (recipients: Seq[ActorSelection], marker: String) => {
        val probe = TestProbe()
        implicit val sender: ActorRef = probe.ref

        val fut = ConcurrentFunctor[StartMessage.type]({ _: StartMessage.type => Thread.sleep(500); MessageA.Request("test message A")}, Seq(partnerDactor1, partnerDactor2))
          .and({ _: StartMessage.type => MessageB.Request()}, Seq(partnerDactor1, partnerDactor2))
          .collect(identity)

        val functorRef = startConcurrentFunctor(fut, system)(StartMessage)
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

    "having multiple steps" should {

      val fut = SequentialFunctor()
        .start( (_: StartMessage.type) => MessageA.Request("multistep functor: message 1"), Seq(partnerDactor1))
        .next( _ => MessageA.Request("multistep functor: message 2"), Seq(partnerDactor2))
        .next( _ => MessageA.Request("multistep functor: message 3"), Seq(partnerDactor3))
        .end(identity)

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
