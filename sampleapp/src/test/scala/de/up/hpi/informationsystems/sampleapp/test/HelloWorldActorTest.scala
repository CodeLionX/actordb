package de.up.hpi.informationsystems.sampleapp.test

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import de.up.hpi.informationsystems.sampleapp.Worker
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.language.implicitConversions


class HelloWorldActorTest(_system: ActorSystem)
  extends TestKit(_system)
    with Matchers
    with FlatSpecLike
    with BeforeAndAfterAll {

  def this() = this(ActorSystem("TestSpec"))

  override def afterAll: Unit = {
    shutdown(system)
  }

  "A worker actor" should "reply with his name" in {
    val probe = TestProbe()
    val workerActor = system.actorOf(Worker.props("Name"))

    workerActor.tell(Worker.ReadName, probe.ref)
    val response = probe.expectMsgType[Worker.RespondName]
    response.name should === (Some("Name"))
  }

}
