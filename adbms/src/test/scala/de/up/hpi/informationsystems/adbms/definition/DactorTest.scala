package de.up.hpi.informationsystems.adbms.definition

import akka.actor.{ActorNotFound, ActorRef, ActorSystem}
import akka.testkit.TestKit
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.Dactor
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.FailureMessage
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

object DactorTest {
  class TestDactor(id: Int) extends Dactor(id) {

    override val relations: Map[String, Relation] = Map()

    override def receive: Receive = {
      case _ => {}
    }
  }
}

class DactorTest extends TestKit(ActorSystem("test-system"))
  with WordSpecLike
  with Matchers
  with ScalaFutures {

  "Dactor" can {

    implicit val timeout: Timeout = 1.seconds

    "using Dactor companion object" should {

      "create new dactors of requested type using .dactorOf" in {
        val testDactor = Dactor.dactorOf(system, classOf[DactorTest.TestDactor], 1)
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
  }

}
