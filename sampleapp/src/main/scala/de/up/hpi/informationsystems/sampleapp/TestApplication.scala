package de.up.hpi.informationsystems.sampleapp

import akka.actor.{Actor, ActorSystem, Props}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.adbms.protocols.DefaultMessagingProtocol
import de.up.hpi.informationsystems.sampleapp.dactors.GroupManager

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Success

object TestApplication extends App {
  val system = ActorSystem("system")

  try {
    val tester = system.actorOf(TestDactor.props(1), "TestDactor-1")
    val groupManager10 = Dactor.dactorOf(system, classOf[GroupManager], 10)

    val userCols: Set[UntypedColumnDef] = Set(
      ColumnDef[String]("Firstname"),
      ColumnDef[String]("Lastname"),
      ColumnDef[Int]("Age")
    )
    tester.tell(DefaultMessagingProtocol.InsertIntoRelation("User", Seq(Record(userCols)(
      ColumnDef[String]("Firstname") ~> "Somebody" &
      ColumnDef[String]("Lastname") ~> "I used to know" &
      ColumnDef[Int]("Age") ~> 12
    ).build())), Actor.noSender)
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
  def props(id: Int): Props = Props(new TestDactor(id))

  case class Test()
  case class Terminate()

  /**
    * Definition of Columns and Relations for relation "User"
    */
  object User extends RelationDef {
    val colFirstname: ColumnDef[String] = ColumnDef("Firstname")
    val colLastname: ColumnDef[String] = ColumnDef("Lastname")
    val colAge: ColumnDef[Int] = ColumnDef("Age")

    override val columns: Set[UntypedColumnDef] = Set(colFirstname, colLastname, colAge)
    override val name: String = "User"
  }

  /**
    * Definition of Columns and Relations for relation "Customer"
    */
  object Customer extends RelationDef {
    val colId: ColumnDef[Int] = ColumnDef("Id")
    val colName: ColumnDef[String] = ColumnDef("Name")
    val colDiscount: ColumnDef[Double] = ColumnDef("Discount")

    override val columns: Set[UntypedColumnDef] = Set(colId, colName, colDiscount)
    override val name: String = "Customer"
  }
}

class TestDactor(id: Int) extends Dactor(id) {
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

  override protected val relations: Map[RelationDef, MutableRelation] =
    Dactor.createAsRowRelations(Seq(User, Customer))

  override def receive: Receive = {
    case Test => test()
    case Terminate() => context.system.terminate()
  }

  def test(): Unit = {
    /**
      * Testing User relation => ColumnStore
      */

    println(relations(User).columns.mkString(", "))
    relations(User).insert(
      Record(User.columns)
        .withCellContent(User.colLastname)("Maier")
        .withCellContent(User.colFirstname)("Hans")
        .withCellContent(User.colAge)(33)
        .build()
    )
    relations(User).insert(
      Record(User.columns)
        .withCellContent(User.colFirstname)("Hans")
        .withCellContent(User.colLastname)("Schneider")
        .withCellContent(User.colAge)(12)
        .build()
    )
    relations(User).insert(
      Record(User.columns)
        .withCellContent(User.colFirstname)("Justus")
        .withCellContent(User.colLastname)("Jonas")
        .build()
    )
    println()
    println(relations(User))

    println()
    println("where:")
    println(
      relations(User).where[String](User.colFirstname -> { _ == "Hans" }).records.getOrElse(Seq.empty).pretty
    )
    println("whereAll:")
    println(
      relations(User)
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
    println(relations(User).project(Set(User.colFirstname, User.colLastname)).records.getOrElse(Seq.empty).pretty)


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

    println(relations(Customer).columns.mkString(", "))
    println()
    relations(Customer).insert(
      Record(Customer.columns)
        .withCellContent(Customer.colId)(1)
        .withCellContent(Customer.colName)("BMW")
        .build()
    )
    relations(Customer).insert(
      Record(Customer.columns)
        .withCellContent(Customer.colId)(2)
        .withCellContent(Customer.colName)("BMW Group")
        .withCellContent(Customer.colDiscount)(0.023)
        .build()
    )
    relations(Customer).insert(
      Record(Customer.columns)
        .withCellContent(Customer.colId)(3)
        .withCellContent(Customer.colName)("Audi AG")
        .withCellContent(Customer.colDiscount)(0.05)
        .build()
    )

    println(relations(Customer))
    println("where:")
    println(
      relations(Customer).where[String](Customer.colName -> { _.contains("BMW") }).records.getOrElse(Seq.empty).pretty
    )

    val isBMW: Any => Boolean = value =>
      value.asInstanceOf[String].contains("BMW")

    def discountGreaterThan(threshold: Double): Any => Boolean = value =>
      value.asInstanceOf[Double] >= threshold

    println("whereAll:")
    println(
      relations(Customer)
        .whereAll(
          Map(Customer.colName.untyped -> isBMW)
            ++ Map(Customer.colDiscount.untyped -> discountGreaterThan(0.01))
        )
        .records.getOrElse(Seq.empty)
        .pretty
    )

    println()
    println("Projection of customer relation:")
    println(relations(Customer).project(Set(Customer.colName, Customer.colDiscount)).records.getOrElse(Seq.empty).pretty)

    /**
      * Testing communicating with another actor
      */

    val groupManager10 = dactorSelection(classOf[GroupManager], 10)
    println(groupManager10)
    groupManager10 ! GroupManager.GetFixedDiscounts.Request(Seq(1,2,3))
  }

}
