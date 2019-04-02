package de.up.hpi.informationsystems.adbms.function

import akka.actor._
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.RelationDef
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

  sealed trait EndMessageType extends RequestResponseProtocol.Message
  case class EndMessage(result: Relation) extends RequestResponseProtocol.Success[EndMessageType]

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

class ConcurrentFunctorTest extends TestKit(ActorSystem("concurrent-functor-test-system"))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  import ConcurrentFunctorTest._

  override def afterAll(): Unit = shutdown(system)

  "A ConcurrentFunction" when {

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

        trait ResultCollector[E <: Success[_]] {
          def collect(result: Success[_ <: Message], resultEmitter: E => Unit)
        }

        class ConcurrentFunctorBuilder[S <: Request[_]: ClassTag]
                                      (jobs: Seq[(S => Request[_ <: Message], Seq[ActorSelection])]) {
          def and(job: S => Request[_ <: Message], recipients: Seq[ActorSelection]): ConcurrentFunctorBuilder[S] =
            new ConcurrentFunctorBuilder[S](jobs :+ (job, recipients))

          def collect[E <: Success[_]: ClassTag]
                     (end: ResultCollector[E]): ConcurrentFunctorDef[S, E] =
            new ConcurrentFunctorDef[S, E](jobs, end)
        }

        class ConcurrentFunctorDef[S <: Request[_]: ClassTag, E <: Success[_]: ClassTag]
                                  (jobs: Seq[(S => Request[_ <: Message], Seq[ActorSelection])],
                                   resultCollector: ResultCollector[E]) {
          def props: Props = Props(new ConcurrentFunctor[S, E](jobs, resultCollector))
        }
      }

      class ConcurrentFunctor[S <: Request[_]: ClassTag, E <: Success[_]: ClassTag]
                             (jobs: Seq[(S => Request[_ <: Message], Seq[ActorSelection])],
                              resultCollector: ConcurrentFunctor.ResultCollector[E]) extends Actor {
        var backTo: ActorRef = Actor.noSender
        var results: Seq[Success[Message]] = Seq.empty
        var resultSize = 0

        override def receive: Receive = {
          case s: S =>
            jobs.foreach{ case (f, actorSelections) =>
              actorSelections foreach ( _ ! f(s) )
            }
            resultSize = jobs.map(_._2.size).sum
            backTo = sender

          case message: Success[_] =>
            results :+= message

            if(results.size == resultSize) {
              val emitter: E => Unit = returnMessage => {
                backTo ! returnMessage
              }

              results.foreach( res =>
              resultCollector.collect(res, emitter)
              )
            }
        }
      }

      def startConcurrentFunctor[S <: Request[_]](function: ConcurrentFunctor.ConcurrentFunctorDef[S, _], refFactory: ActorRefFactory)
                                                 (message: S)
                                                 (implicit sender: ActorRef): ActorRef = {
        val ref = refFactory.actorOf(function.props)
        ref.tell(message, sender)
        ref
      }

      val testSimpleConFunctor = (recipients: Seq[ActorSelection], marker: String) => {
        val probe = TestProbe()
        implicit val sender: ActorRef = probe.ref

        val fut = ConcurrentFunctor[StartMessage.type]({ _: StartMessage.type => Thread.sleep(500); MessageA.Request("test message A")}, Seq(partnerDactor1, partnerDactor2))
          .and({ _: StartMessage.type => MessageB.Request()}, Seq(partnerDactor1, partnerDactor2))
          .collect( new ConcurrentFunctor.ResultCollector[EndMessage] {

            var counter = 0

            def collect(result: Success[_ <: Message], resultEmitter: EndMessage => Unit): Unit = {
              counter += 1
              if(counter == 4){
                resultEmitter(EndMessage(Relation.empty))
              }
            }
          })

        val functorRef = startConcurrentFunctor(fut, system)(StartMessage)
        probe.watch(functorRef)
        probe.expectMsg(EndMessage(Relation.empty))
      }

      "handle one receiver correctly" in {
        val recipients = Seq(partnerDactor1)
        testSimpleConFunctor(recipients, "one receiver")
      }

      "handle different receivers correctly" in {
        val recipients = Seq(partnerDactor1, partnerDactor2, partnerDactor3)
        testSimpleConFunctor(recipients, "different receivers")
      }

      "handle the same receiver multiple times correctly" in {
        val recipients = Seq(partnerDactor1, partnerDactor1, partnerDactor1)
        testSimpleConFunctor(recipients, "same receiver multiple times")
      }
    }
  }

}
