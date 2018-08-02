import Util.SequentialFunction.SequentialFunctionBuilderCompleted
import Util.TestPartner.TestRelation

import scala.language.higherKinds
import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props, Terminated}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef}
import de.up.hpi.informationsystems.adbms.protocols.{DefaultMessageHandling, DefaultMessagingProtocol}
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, Relation, SingleRowRelation}
import de.up.hpi.informationsystems.fouleggs.dactors.Person

import scala.reflect.ClassTag

//==================================================

object Util extends App {

  // Prerequisite changes

  trait Message

  trait Request[T <: Message]

  sealed trait Response[+T <: Message] {
    val rel: Relation
  }

  trait Success[+T <: Message] extends Response[T]
  trait Failure[+T <: Message] extends Response[T]


  // SequentialFunction CO

  object SequentialFunction {
    def apply(): SequentialFunctionBuilderBare = new SequentialFunctionBuilderBare()

    class SequentialFunctionBuilderBare {
      def start[A <: Request[_]: ClassTag, L <: Message](start: A => Request[L], recipients: Seq[ActorSelection]): SequentialFunctionBuilderStarted[A, L] =
        new SequentialFunctionBuilderStarted[A, L]((start, recipients), Seq.empty)
    }

    class SequentialFunctionBuilderStarted[A <: Request[_]: ClassTag, L <: Message](start: (A => Request[_], Seq[ActorSelection]), steps: Seq[(Response[Message] => Request[Message], Seq[ActorSelection])]) {
      def next[N <: Message](transform: Response[L] => Request[N], recipients: Seq[ActorSelection]): SequentialFunctionBuilderStarted[A, N] =
        new SequentialFunctionBuilderStarted[A, N](start, steps :+ (transform.asInstanceOf[Response[Message] => Request[Message]], recipients))

      def end[O <: Message](end: Response[L] => Response[O]): SequentialFunctionBuilderCompleted[A, Response[O]] =
        new SequentialFunctionBuilderCompleted[A, Response[O]](start, steps, end.asInstanceOf[Response[Message] => Response[O]])
    }

    class SequentialFunctionBuilderCompleted[A <: Request[_]: ClassTag, O <: Response[_]: ClassTag](start: (A => Request[_], Seq[ActorSelection]), steps: Seq[(Response[Message] => Request[Message], Seq[ActorSelection])], end: Response[Message] => O) {
      def props: Props = Props(new SequentialFunction[A, O](start, steps, end))
    }
  }

  // SequentialFunction class

  class SequentialFunction[S <: Request[_]: ClassTag, E <: Response[_]: ClassTag]
                          (start: (S => Request[_], Seq[ActorSelection]),
                           steps: Seq[(Response[Message] => Request[Message], Seq[ActorSelection])],
                           end: Response[Message] => E) extends Actor {
    override def receive: Receive = startReceive()

    def startReceive(): Receive = {
      case startMessage: S =>
        val request = start._1(startMessage)
        start._2 foreach { _ ! request }
        context.become(awaitResponsesReceive(start._2.length, Seq.empty, steps, sender()))
    }

    def awaitResponsesReceive[C <: Response[Message]](totalResponses: Int,
                              receivedResponses: Seq[Response[Message]],
                              pendingSteps: Seq[(Response[Message] => Request[Message], Seq[ActorSelection])],
                              backTo: ActorRef): Receive = {
      case message: Response[Message] =>  // TODO matches any Response message
        if ((totalResponses - (receivedResponses :+ message).length) > 0) {
          context.become(awaitResponsesReceive[C](totalResponses, receivedResponses :+ message, pendingSteps, backTo))
        } else {
          val constructor = message.getClass.getConstructors()(0)
          val unionRelation = (receivedResponses :+ message)
            .foldLeft(constructor.newInstance(Relation.empty).asInstanceOf[Response[Message]])((a: Response[Message], b: Response[Message]) =>
              constructor.newInstance(a.rel.unionAll(b.rel)).asInstanceOf[Response[Message]])
          if (pendingSteps.nonEmpty) {
            self ! unionRelation
            context.become(nextReceive(pendingSteps.head._1, pendingSteps.head._2, steps.drop(1), backTo))
          } else {
            self ! unionRelation
            context.become(endReceive(backTo))
          }
        }
    }

    def nextReceive(currentFunction: Util.Response[Util.Message] => Util.Request[Util.Message],
                    currentRecipients: Seq[ActorSelection],
                    pendingSteps: Seq[(Util.Response[Util.Message] => Util.Request[Util.Message], Seq[ActorSelection])],
                    backTo: ActorRef): Receive = {
      case message: Response[Message] =>  // TODO matches any Response message
        val request = currentFunction(message)
        currentRecipients foreach { _ ! request }
        context.become(awaitResponsesReceive(currentRecipients.length, Seq.empty, pendingSteps, backTo))
    }

    def endReceive(backTo: ActorRef): Receive = {
      case message: Response[Message] =>
        println(s"Sending ${end(message)} to $backTo")
        backTo ! end(message)
        context.stop(self)
    }

    def props: Props = Props[SequentialFunction[S, E]]
  }


  // Test setup

  object Op1 {
    sealed trait MessageA extends Message

    case class Req(variable: Int) extends Request[MessageA]
    case class Res(rel: Relation) extends Response[MessageA]
  }

  object Op2 {
    sealed trait MessageB extends Message

    case class Req() extends Request[MessageB]
    case class Res(rel: Relation) extends Response[MessageB]
  }

  class AUT() extends Actor {

    override def receive: Receive = {
      // TODO this will only be alleviable using reflection -> extra dependency
      case m: Response[Op2.MessageB] => println(s"fuck ${m.rel}")
      case m: Response[Op1.MessageA] => println(s"Seq fun done: ${m.rel}")

      case m: Op1.Req =>
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

    private val mySequentialFunction = SequentialFunction()
      .start((a: Request[Op1.MessageA]) => {
        println("start step")
        a
      }, recipients)
      .next(a => {
        println("next step")
        Op2.Req()
      }, recipients)
      .end(a => {
        println("end step")
        a
      })

    private val sfoo = SequentialFunction()
      .start((a: Request[Op1.MessageA]) => {
        a
      }, recipients)
      .next(b => Op1.Req(2), recipients)

    def startSequentialFunction[S <: Request[_]](function: SequentialFunctionBuilderCompleted[S, _])(message: S): ActorRef = {
      val ref = context.system.actorOf(function.props)
      ref ! message
      ref
    }
  }

  object TestPartner {
    import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._

    object TestRelation extends RelationDef {
      val partnerId: ColumnDef[Int] = ColumnDef[Int]("partnerId")
      val value: ColumnDef[Int] = ColumnDef[Int]("value")

      override val name: String = "testrelation"
      override val columns: Set[UntypedColumnDef] = Set(partnerId, value)
    }

    def props(id: Int): Props = Props(new TestPartner(id))
  }

  class TestPartner(id: Int) extends TestPartnerBase(id) with DefaultMessageHandling

  class TestPartnerBase(id: Int) extends Dactor(id) {
    override def receive: Receive = {
      case message: Op1.Req => sender() ! Op1.Res(relations(TestRelation).immutable)
      case message: Op2.Req => sender() ! Op2.Res(relations(TestRelation).immutable)
    }

    override protected val relations: Map[RelationDef, MutableRelation] = Map(TestRelation -> SingleRowRelation(TestRelation))
  }

  // Test execution

  lazy val actorSystem: ActorSystem = ActorSystem("test-system")
  val d = actorSystem.actorOf(Props[AUT])
  val partner1 = actorSystem.actorOf(TestPartner.props(1), "testpartner1")
  partner1 ! DefaultMessagingProtocol.InsertIntoRelation(TestRelation.name, Seq(TestRelation.newRecord
    .withCellContent(TestRelation.partnerId)(1)
    .withCellContent(TestRelation.value)(12)
    .build()
  ))

  val partner2 = actorSystem.actorOf(TestPartner.props(2), "testpartner2")
  partner2 ! DefaultMessagingProtocol.InsertIntoRelation(TestRelation.name, Seq(TestRelation.newRecord
    .withCellContent(TestRelation.partnerId)(2)
    .withCellContent(TestRelation.value)(11)
    .build()
  ))

  d ! Op1.Req(1)

}

