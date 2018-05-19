package de.up.hpi.informationsystems.sampleapp.dactors.test

import akka.actor.{Actor, ActorNotFound, ActorRef, ActorSystem}
import akka.testkit.TestKit
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.sampleapp.dactors.{Cart, Customer}
import org.scalatest.{Matchers, WordSpecLike}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.exceptions.TestFailedException

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class CartTest
  extends TestKit(ActorSystem("test-system"))
    with WordSpecLike
    with Matchers
    with ScalaFutures {

  "A cart" should {

    implicit val timeout: Timeout = 1.seconds
    val cart = Dactor.dactorOf(system, classOf[Cart], 1)

    "exist" in {
      val future = Dactor.dactorSelection(system, classOf[Cart], 1).resolveOne()
      ScalaFutures.whenReady(future) { s =>
        s shouldBe a [ActorRef]
      }
    }

    "not exist" in {
      val future = Dactor.dactorSelection(system, classOf[Cart], 2).resolveOne()
      ScalaFutures.whenReady(future.failed) { e =>
        e shouldBe a [ActorNotFound]
      }
    }

  }

}
