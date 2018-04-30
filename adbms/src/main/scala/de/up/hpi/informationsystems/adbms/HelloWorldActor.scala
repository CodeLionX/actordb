package de.up.hpi.informationsystems.adbms

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}

import scala.io.StdIn

object HelloWorldActor {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("hw-system")
    val workers = Seq(
      "Hans Walter",
      "Peter Maier",
      "Juliane Künstler"
    )

    try {
      val supervisor = system.actorOf(Supervisor.props(), "hw-supervisor")
      workers.foreach( name =>
        supervisor.tell(Supervisor.RequestRegisterWorker(name), system.deadLetters)
      )

      // wait until ENTER is pressed before exiting
      println(">>>>>> Press ENTER to greet workers")
      StdIn.readLine()
      supervisor.tell(Supervisor.GreetWorkers, system.deadLetters)
    } finally {
      system.terminate()
    }
  }
}

object Supervisor {
  def props(): Props = Props(new Supervisor)

  final case class RequestRegisterWorker(name: String)
  case object WorkerRegistered

  case object GreetWorkers
  case object WorkerGreeted
}

class Supervisor extends Actor with ActorLogging {
  import Supervisor._

  var workers = Seq.empty[ActorRef]

  override def preStart(): Unit = log.info("Supervisor started")

  override def postStop(): Unit = log.info("Supervisor stopped")

  def registerWorker(name: String) = {
    val worker = context.actorOf(Worker.props(name))
    workers ++= Seq(worker)
    context.watch(worker)
  }

  override def receive: Receive = {
    case GreetWorkers =>
      log.info("Greeting workers..")
      workers.foreach(_ ! Worker.ReadName)

    case Worker.RespondName(optName) =>
      log.info(s"received worker name '$optName' from $sender")
      optName.foreach(name => println(s">>>>>> Hello $name"))

    case RequestRegisterWorker(name) =>
      registerWorker(name)
      sender ! WorkerRegistered

  }
}


object Worker {
  def props(name: String): Props = Props(new Worker(name))

  case object ReadName
  final case class RespondName(name: Option[String])
}

class Worker(name: String) extends Actor with ActorLogging {
  import Worker._

  override def preStart(): Unit = log.info("Worker with name {} started", name)

  override def postStop(): Unit = log.info("Worker with name {} stopped", name)

  override def receive: Receive = {
    case ReadName =>
        log.info(s"Received a ReadName message, sending response")
        sender ! RespondName(Some(name))
  }
}