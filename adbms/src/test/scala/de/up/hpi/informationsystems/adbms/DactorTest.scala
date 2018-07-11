package de.up.hpi.informationsystems.adbms

import java.util.NoSuchElementException

import akka.actor.{Actor, ActorNotFound, ActorRef, ActorSystem, InvalidActorNameException}
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef, UntypedColumnDef}
import de.up.hpi.informationsystems.adbms.protocols.{DefaultMessageHandling, DefaultMessagingProtocol}
import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation.MutableRelation
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps

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

      "prefilled relations available" should {
        import DactorTest.DactorWithRelation.TestRelation

        val probe = TestProbe()
        val dut = Dactor.dactorOf(system, classOf[DactorTest.DactorWithRelation], 2)

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

        "return an immutable copy of existing relation" in {
          val queryMessage = DefaultMessagingProtocol.SelectAllFromRelation.Request(TestRelation.name)
          dut.tell(queryMessage, probe.ref)
          val response = probe.expectMsgType[DefaultMessagingProtocol.SelectAllFromRelation.Success]
          response.relation.records shouldEqual util.Success(Seq(
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
        }

        "fail on requests for non existing relations" in {
          val queryMessage = DefaultMessagingProtocol.SelectAllFromRelation.Request("fail_please")
          dut.tell(queryMessage, probe.ref)
          val failure = probe.expectMsgType[DefaultMessagingProtocol.SelectAllFromRelation.Failure]
          failure.cause shouldBe a[NoSuchElementException]
        }
      }
    }
  }

}
