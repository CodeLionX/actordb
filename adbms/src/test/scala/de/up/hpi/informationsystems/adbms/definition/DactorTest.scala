package de.up.hpi.informationsystems.adbms.definition

import java.util.NoSuchElementException

import akka.actor.{Actor, ActorNotFound, ActorRef, ActorSystem, InvalidActorNameException, Props}
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.protocols.DefaultMessagingProtocol
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps

object DactorTest {
  class TestDactor(id: Int) extends Dactor(id) {

    override val relationFromDef: Map[RelationDef, MutableRelation] = Map.empty

    override def receive: Receive = Actor.emptyBehavior
  }

  class TestDactor2(id: Int) extends Dactor(id) {

    override val relationFromDef: Map[RelationDef, MutableRelation] = Map.empty

    override def receive: Receive = Actor.emptyBehavior
  }

  object DactorWithRelation {
    def props(id: Int): Props = Props(new DactorWithRelation(id))

    object TestRelation extends RelationDef {
      val col1: ColumnDef[Int] = ColumnDef("col1")
      val col2: ColumnDef[String] = ColumnDef("col2")

      override val columns: Set[UntypedColumnDef] = Set(col1, col2)
      override val name: String = "testRelation"
    }

  }

  class DactorWithRelation(id: Int) extends Dactor(id) {
    import DactorWithRelation._

    override protected val relationFromDef: Map[RelationDef, MutableRelation] =
      Dactor.createAsRowRelations(Seq(TestRelation))

    override def receive: Receive = Actor.emptyBehavior
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

    "no relations" should {

      "reject insert messages with a Failure" in {
        val probe = TestProbe()
        val dut = Dactor.dactorOf(system, classOf[DactorTest.TestDactor], 99)

        val msg = DefaultMessagingProtocol.InsertIntoRelation("someRelation", Seq(Record.empty))
        dut.tell(msg, probe.ref)
        val response = probe.expectMsgType[akka.actor.Status.Failure]
        response.cause shouldBe a[NoSuchElementException]
        response.cause.getMessage should startWith("key not found")
      }
    }

    "relations available" should {
      import DactorTest.DactorWithRelation.TestRelation
      import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._

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

}
