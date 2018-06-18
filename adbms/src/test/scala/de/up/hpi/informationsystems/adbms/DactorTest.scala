package de.up.hpi.informationsystems.adbms

import java.util.NoSuchElementException

import akka.actor.{Actor, ActorNotFound, ActorRef, ActorSystem, InvalidActorNameException}
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.DactorTest.DactorWithRequestResponseBehaviour
import de.up.hpi.informationsystems.adbms.DactorTest.DactorWithRequestResponseBehaviour.{RemindMe, SqrtMsg}
import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef, UntypedColumnDef}
import de.up.hpi.informationsystems.adbms.protocols.{DefaultMessageHandling, DefaultMessagingProtocol, RequestResponseProtocol}
import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, Relation}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Success, Try}

object DactorTest {
  class TestDactor(id: Int) extends Dactor(id) {

    override val relations: Map[RelationDef, MutableRelation] = Map.empty

    override def receive: Receive = Actor.emptyBehavior
  }

  class TestDactorDefault(id: Int) extends TestDactor(id) with DefaultMessageHandling

  class TestDactor2(id: Int) extends Dactor(id) {

    override val relations: Map[RelationDef, MutableRelation] = Map.empty

    override def receive: Receive = Actor.emptyBehavior
  }

  object DactorWithRelation {

    object TestRelation extends RelationDef {
      val col1: ColumnDef[Int] = ColumnDef("col1")
      val col2: ColumnDef[String] = ColumnDef("col2")

      override val columns: Set[UntypedColumnDef] = Set(col1, col2)
      override val name: String = "testRelation"
    }

    class DactorWithRelationBase(id: Int) extends Dactor(id) {
      override protected val relations: Map[RelationDef, MutableRelation] =
        Dactor.createAsRowRelations(Seq(TestRelation))

      override def receive: Receive = Actor.emptyBehavior
    }
  }

  class DactorWithRelation(id: Int) extends DactorWithRelation.DactorWithRelationBase(id) with DefaultMessageHandling

  object DactorWithRequestResponseBehaviour {
    object SqrtMsg {
      case class Request(value: Int) extends RequestResponseProtocol.Request
      case class Success(result: Relation) extends RequestResponseProtocol.Success
      case class Failure(e: Throwable) extends RequestResponseProtocol.Failure
    }

    object RemindMe {
      case class Request(reminder: String, time: Duration) extends RequestResponseProtocol.Request
      case class Success(result: Relation) extends RequestResponseProtocol.Success
      case class Failure(e: Throwable) extends RequestResponseProtocol.Failure
    }

    val sqrtColDef: ColumnDef[Double] = ColumnDef[Double]("sqrt")
    val reminderColDef: ColumnDef[String] = ColumnDef[String]("reminder")
  }

  class DactorWithRequestResponseBehaviour(id: Int) extends Dactor(id) {
    /**
      * Returns a map of relation definition and corresponding relational store.
      *
      * @return map of relation definition and corresponding relational store
      */
    override protected val relations: Map[RelationDef, MutableRelation] = Map.empty

    def calculateSqrt(value: Int): Try[Double] = Try{
      if (value >= 0) Math.sqrt(value)
      else throw new RuntimeException(s"Cannot calculate square root of negative value $value")
    }

    override def receive: Receive = {
      case SqrtMsg.Request(value: Int) => calculateSqrt(value) match {
        case scala.util.Success(sqrt) => sender() ! DactorWithRequestResponseBehaviour.SqrtMsg.Success(Relation(Seq(Record(Set(DactorWithRequestResponseBehaviour.sqrtColDef.untyped))(DactorWithRequestResponseBehaviour.sqrtColDef ~> sqrt).build())))
        case scala.util.Failure(e) => sender() ! DactorWithRequestResponseBehaviour.SqrtMsg.Failure(e)
      }

      case RemindMe.Request(reminder, time) => {
        Thread.sleep(time.toMillis)
        sender() ! RemindMe.Success(Relation(Seq(Record(Set(DactorWithRequestResponseBehaviour.reminderColDef.untyped))(DactorWithRequestResponseBehaviour.reminderColDef ~> reminder).build())))
      }
    }
  }
}

class DactorTest extends TestKit(ActorSystem("test-system"))
  with WordSpecLike
  with Matchers
  with ScalaFutures
  with BeforeAndAfterAll {

  override def afterAll: Unit = {
    shutdown(system)
  }

  "Dactor" when {

    implicit val timeout: Timeout = 1 second

    "using Dactor companion object" should {

      "create new dactors of requested type using .dactorOf" in {
        noException should be thrownBy Dactor.dactorOf(system, classOf[DactorTest.TestDactor], 1)
      }

      "enforce unique id for the same classTag when using .dactorOf" in {
        an [InvalidActorNameException] should be thrownBy Dactor.dactorOf(system, classOf[DactorTest.TestDactor], 1)
      }

      "allow equal ids for different classes" in {
        noException should be thrownBy Dactor.dactorOf(system, classOf[DactorTest.TestDactor2], 1)
      }

      "find existing dactors using .dactorSelection(...).resolve" in {
        val future = Dactor.dactorSelection(system, classOf[DactorTest.TestDactor], 1).resolveOne()
        ScalaFutures.whenReady(future) { ref =>
          ref shouldBe a [ActorRef]
        }
      }

      "fail to find dactors using .dactorSelection(...).resolve for non existing dactors" in {
        val future = Dactor.dactorSelection(system, classOf[DactorTest.TestDactor], 2).resolveOne()
        ScalaFutures.whenReady(future.failed) { e =>
          e shouldBe a [ActorNotFound]
        }
      }
    }

    "mixed in default message handler" when {

      "no relations" should {
        val probe = TestProbe()
        val dut = Dactor.dactorOf(system, classOf[DactorTest.TestDactorDefault], 99)

        "reject insert messages with a Failure" in {

          val msg = DefaultMessagingProtocol.InsertIntoRelation("someRelation", Seq(Record.empty))
          dut.tell(msg, probe.ref)
          val response = probe.expectMsgType[akka.actor.Status.Failure]
          response.cause shouldBe a[NoSuchElementException]
          response.cause.getMessage should startWith("key not found")
        }
      }

      "relations available" should {
        import DactorTest.DactorWithRelation.TestRelation

        val probe = TestProbe()
        val dut = Dactor.dactorOf(system, classOf[DactorTest.DactorWithRelation], 1)

        "insert matching record successfully" in {
          val insertMessage = DefaultMessagingProtocol.InsertIntoRelation(TestRelation.name, Seq(
            Record(TestRelation.columns)(
              TestRelation.col1 ~> 1 &
                TestRelation.col2 ~> "1"
            ).build()
          ))
          dut.tell(insertMessage, probe.ref)
          probe.expectMsg(akka.actor.Status.Success)
        }

        "insert multiple matching records successfully" in {
          val insertMessage = DefaultMessagingProtocol.InsertIntoRelation(TestRelation.name, Seq(
            Record(TestRelation.columns)(
              TestRelation.col1 ~> 1 &
                TestRelation.col2 ~> "1"
            ).build(),
            Record(TestRelation.columns)(
              TestRelation.col1 ~> 2 &
                TestRelation.col2 ~> "2"
            ).build(),
            Record(TestRelation.columns)(
              TestRelation.col1 ~> 3 &
                TestRelation.col2 ~> "3"
            ).build()
          ))
          dut.tell(insertMessage, probe.ref)
          probe.expectMsg(akka.actor.Status.Success)
        }

        "fail inserting wrong records" in {
          val insertMessage = DefaultMessagingProtocol.InsertIntoRelation(TestRelation.name, Seq(
            Record(Set(TestRelation.col1, ColumnDef[Float]("undefinedCol")))(
              TestRelation.col1 ~> 1 &
                ColumnDef[Float]("undefinedCol") ~> 1.2f
            ).build()
          ))
          dut.tell(insertMessage, probe.ref)
          val response = probe.expectMsgType[akka.actor.Status.Failure]
          response.cause shouldBe a[IncompatibleColumnDefinitionException]
        }
      }
    }

    "supporting RequestResponseProtocol" should {

      Dactor.dactorOf(system, classOf[DactorTest.DactorWithRequestResponseBehaviour], 1)

      "answer an askDactor call with the corresponding Success message when receiving a request" in {
        val fr = Dactor.askDactor(system, classOf[DactorTest.DactorWithRequestResponseBehaviour], Map(1 -> DactorTest.DactorWithRequestResponseBehaviour.SqrtMsg.Request(16)))
        fr.records shouldEqual Success(Relation(Seq(
          Record(Set(DactorTest.DactorWithRequestResponseBehaviour.sqrtColDef))(DactorWithRequestResponseBehaviour.sqrtColDef ~> 4).build()
        )).records.get)
      }

      "answer an askDactor call with the corresponding Failure message when receiving a bad request" in {
        val fr = Dactor.askDactor(system, classOf[DactorTest.DactorWithRequestResponseBehaviour], Map(1 -> DactorTest.DactorWithRequestResponseBehaviour.SqrtMsg.Request(-1)))
        fr.records shouldEqual scala.util.Failure(new RuntimeException("Cannot calculate square root of negative value -1"))
      }

      "timeout an askDactor call for a request that takes too long to answer" in {
        val fr = Dactor.askDactor(system, classOf[DactorTest.DactorWithRequestResponseBehaviour], Map(1 -> DactorTest.DactorWithRequestResponseBehaviour.RemindMe.Request("Wake up!", 2 seconds)))
        // FIXME should time out
        fr.records shouldEqual scala.util.Failure(new RuntimeException("Cannot calculate square root of negative value -1"))
      }
    }
  }

}
