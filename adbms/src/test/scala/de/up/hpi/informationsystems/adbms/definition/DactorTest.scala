package de.up.hpi.informationsystems.adbms.definition

import java.util.NoSuchElementException

import akka.actor.{Actor, ActorNotFound, ActorRef, ActorSystem, InvalidActorNameException}
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.protocols.DefaultMessagingProtocol
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

object DactorTest {
  class TestDactor(id: Int) extends Dactor(id) {

    override val relations: Map[String, MutableRelation] = Map()

    override def receive: Receive = Actor.emptyBehavior
  }

  class TestDactor2(id: Int) extends Dactor(id) {

    override val relations: Map[String, MutableRelation] = Map()

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

    implicit val timeout: Timeout = 1.seconds

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
         response.cause shouldBe a [NoSuchElementException]
         response.cause.getMessage should startWith ("key not found")
       }
    }
  }

}
