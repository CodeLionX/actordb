package de.up.hpi.informationsystems.sampleapp

import akka.actor.{Actor, ActorSystem, Props}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.sampleapp.dactors.GroupManager

import scala.concurrent.duration._
import scala.util.Success
import scala.language.postfixOps

object TestApplication extends App {
  val system = ActorSystem("system")

  try {
    val tester = system.actorOf(TestDactor.props("tester1"), "tester")
    val groupManager10 = Dactor.refFor(system, classOf[GroupManager], 10)

    tester.tell(TestDactor.Test, Actor.noSender)

    // shutdown system
    import de.up.hpi.informationsystems.sampleapp.TestDactor.Terminate
    import system.dispatcher
    system.scheduler.scheduleOnce(5 seconds){
      tester ! Terminate()
    }
  } finally {

  }
}


object TestDactor {
  def props(name: String): Props = Props(new TestDactor(name))

  case class Test()
  case class Terminate()
}

class TestDactor(name: String) extends Dactor(name) {
  import TestDactor._

  /**
    * Helper for printing out seqs of records.
    * @param a sequence of records
    */
  implicit class RecordSeqToString(a: Seq[Record]) {
    def pretty: String =
      a.map{ _.values.mkString(", ") }
        .mkString("\n")
  }

  /**
    * Definition of Columns and Relations for relation "User"
    */
  object User extends RowRelation {
    val colFirstname: ColumnDef[String] = ColumnDef("Firstname")
    val colLastname: ColumnDef[String] = ColumnDef("Lastname")
    val colAge: ColumnDef[Int] = ColumnDef("Age")

    override val columns: Set[UntypedColumnDef] = Set(colFirstname, colLastname, colAge)
  }

  /**
    * Definition of Columns and Relations for relation "Customer"
    */
  object Customer extends RowRelation {
    val colId: ColumnDef[Int] = ColumnDef("Id")
    val colName: ColumnDef[String] = ColumnDef("Name")
    val colDiscount: ColumnDef[Double] = ColumnDef("Discount")

    override val columns: Set[UntypedColumnDef] = Set(colId, colName, colDiscount)
  }

  override protected val relations: Map[String, Relation] = Map("User" -> User) ++ Map("Customer" -> Customer)

  override def receive: Receive = {
    case Test => test()
    case Terminate() => context.system.terminate()
    case e => println(s"Received Message:\n$e")
  }

  def test(): Unit = {
    /**
      * Testing User relation => ColumnStore
      */

    println(User.columns.mkString(", "))
    User.insert(
      Record(User.columns)
        .withCellContent(User.colLastname)("Maier")
        .withCellContent(User.colFirstname)("Hans")
        .withCellContent(User.colAge)(33)
        .build()
    )
    User.insert(
      Record(User.columns)
        .withCellContent(User.colFirstname)("Hans")
        .withCellContent(User.colLastname)("Schneider")
        .withCellContent(User.colAge)(12)
        .build()
    )
    User.insert(
      Record(User.columns)
        .withCellContent(User.colFirstname)("Justus")
        .withCellContent(User.colLastname)("Jonas")
        .build()
    )
    println()
    println(User)

    println()
    println("where:")
    println(
      User.where[String](User.colFirstname -> { _ == "Hans" }).records.getOrElse(Seq.empty).pretty
    )
    println("whereAll:")
    println(
      User
        .whereAll(
          Map(User.colFirstname.untyped -> { name: Any => name.asInstanceOf[String] == "Hans" })
            ++ Map(User.colAge.untyped -> { age: Any => age.asInstanceOf[Int] == 33 })
        )
        .records.getOrElse(Seq.empty)
        .pretty
    )

    assert(ColumnDef[String]("Firstname") == User.colFirstname)
    assert(ColumnDef[Int]("Firstname").untyped != User.colFirstname.untyped)

    println()
    println("Projection of user relation:")
    println(User.project(Set(User.colFirstname, User.colLastname)).records.getOrElse(Seq.empty).pretty)


    /**
      * Testing Customer relation => RowStore
      */

    println()
    println()
    import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
    val record = User.newRecord(
      User.colFirstname ~> "Firstname" &
      User.colLastname ~> "Lastname" &
      User.colAge ~> 45
    ).build()
    println(record)
    assert(record.project(Set(User.colAge)) == Success(Record(Set(User.colAge))(User.colAge ~> 45).build()))
    assert(record.project(Set(User.colAge, Customer.colDiscount)).isFailure)

    println(Customer.columns.mkString(", "))
    println()
    Customer.insert(
      Record(Customer.columns)
        .withCellContent(Customer.colId)(1)
        .withCellContent(Customer.colName)("BMW")
        .build()
    )
    Customer.insert(
      Record(Customer.columns)
        .withCellContent(Customer.colId)(2)
        .withCellContent(Customer.colName)("BMW Group")
        .withCellContent(Customer.colDiscount)(0.023)
        .build()
    )
    Customer.insert(
      Record(Customer.columns)
        .withCellContent(Customer.colId)(3)
        .withCellContent(Customer.colName)("Audi AG")
        .withCellContent(Customer.colDiscount)(0.05)
        .build()
    )

    println(Customer)
    println("where:")
    println(
      Customer.where[String](Customer.colName -> { _.contains("BMW") }).records.getOrElse(Seq.empty).pretty
    )

    val isBMW: Any => Boolean = value =>
      value.asInstanceOf[String].contains("BMW")

    def discountGreaterThan(threshold: Double): Any => Boolean = value =>
      value.asInstanceOf[Double] >= threshold

    println("whereAll:")
    println(
      Customer
        .whereAll(
          Map(Customer.colName.untyped -> isBMW)
            ++ Map(Customer.colDiscount.untyped -> discountGreaterThan(0.01))
        )
        .records.getOrElse(Seq.empty)
        .pretty
    )

    println()
    println("Projection of customer relation:")
    println(Customer.project(Set(Customer.colName, Customer.colDiscount)).records.getOrElse(Seq.empty).pretty)

    /**
      * Testing communicating with another actor
      */

    val groupManager10 = dactorSelection(classOf[GroupManager], 10)
    println(groupManager10)
    groupManager10 ! GroupManager.GetFixedDiscounts.Request(Seq(1,2,3))
  }
}
