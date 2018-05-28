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
  object UserDef extends RelationDef {
    val colFirstname: ColumnDef[String] = ColumnDef("Firstname")
    val colLastname: ColumnDef[String] = ColumnDef("Lastname")
    val colAge: ColumnDef[Int] = ColumnDef("Age")

    override val columns: Set[UntypedColumnDef] = Set(colFirstname, colLastname, colAge)
    override val name: String = "User"
  }

  /**
    * Definition of Columns and Relations for relation "Customer"
    */
  object CustomerDef extends RelationDef {
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

  val User = RowRelation(UserDef)
  val Customer = RowRelation(CustomerDef)

  override protected val relations: Map[String, MutableRelation] = Map(UserDef.name -> User) ++ Map(CustomerDef.name -> Customer)

  override def receive: Receive = {
    case Test => test()
    case Terminate() => context.system.terminate()
  }

  def test(): Unit = {
    /**
      * Testing User relation => ColumnStore
      */

    println(User.columns.mkString(", "))
    User.insert(
      Record(UserDef.columns)
        .withCellContent(UserDef.colLastname)("Maier")
        .withCellContent(UserDef.colFirstname)("Hans")
        .withCellContent(UserDef.colAge)(33)
        .build()
    )
    User.insert(
      Record(UserDef.columns)
        .withCellContent(UserDef.colFirstname)("Hans")
        .withCellContent(UserDef.colLastname)("Schneider")
        .withCellContent(UserDef.colAge)(12)
        .build()
    )
    User.insert(
      Record(UserDef.columns)
        .withCellContent(UserDef.colFirstname)("Justus")
        .withCellContent(UserDef.colLastname)("Jonas")
        .build()
    )
    println()
    println(User)

    println()
    println("where:")
    println(
      User.where[String](UserDef.colFirstname -> { _ == "Hans" }).records.getOrElse(Seq.empty).pretty
    )
    println("whereAll:")
    println(
      User
        .whereAll(
          Map(UserDef.colFirstname.untyped -> { name: Any => name.asInstanceOf[String] == "Hans" })
            ++ Map(UserDef.colAge.untyped -> { age: Any => age.asInstanceOf[Int] == 33 })
        )
        .records.getOrElse(Seq.empty)
        .pretty
    )

    assert(ColumnDef[String]("Firstname") == UserDef.colFirstname)
    assert(ColumnDef[Int]("Firstname").untyped != UserDef.colFirstname.untyped)

    println()
    println("Projection of user relation:")
    println(User.project(Set(UserDef.colFirstname, UserDef.colLastname)).records.getOrElse(Seq.empty).pretty)


    /**
      * Testing Customer relation => RowStore
      */

    println()
    println()
    import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
    val record = UserDef.newRecord(
      UserDef.colFirstname ~> "Firstname" &
      UserDef.colLastname ~> "Lastname" &
      UserDef.colAge ~> 45
    ).build()
    println(record)
    assert(record.project(Set(UserDef.colAge)) == Success(Record(Set(UserDef.colAge))(UserDef.colAge ~> 45).build()))
    assert(record.project(Set(UserDef.colAge, CustomerDef.colDiscount)).isFailure)

    println(Customer.columns.mkString(", "))
    println()
    Customer.insert(
      Record(CustomerDef.columns)
        .withCellContent(CustomerDef.colId)(1)
        .withCellContent(CustomerDef.colName)("BMW")
        .build()
    )
    Customer.insert(
      Record(CustomerDef.columns)
        .withCellContent(CustomerDef.colId)(2)
        .withCellContent(CustomerDef.colName)("BMW Group")
        .withCellContent(CustomerDef.colDiscount)(0.023)
        .build()
    )
    Customer.insert(
      Record(CustomerDef.columns)
        .withCellContent(CustomerDef.colId)(3)
        .withCellContent(CustomerDef.colName)("Audi AG")
        .withCellContent(CustomerDef.colDiscount)(0.05)
        .build()
    )

    println(Customer)
    println("where:")
    println(
      Customer.where[String](CustomerDef.colName -> { _.contains("BMW") }).records.getOrElse(Seq.empty).pretty
    )

    val isBMW: Any => Boolean = value =>
      value.asInstanceOf[String].contains("BMW")

    def discountGreaterThan(threshold: Double): Any => Boolean = value =>
      value.asInstanceOf[Double] >= threshold

    println("whereAll:")
    println(
      Customer
        .whereAll(
          Map(CustomerDef.colName.untyped -> isBMW)
            ++ Map(CustomerDef.colDiscount.untyped -> discountGreaterThan(0.01))
        )
        .records.getOrElse(Seq.empty)
        .pretty
    )

    println()
    println("Projection of customer relation:")
    println(Customer.project(Set(CustomerDef.colName, CustomerDef.colDiscount)).records.getOrElse(Seq.empty).pretty)

    /**
      * Testing communicating with another actor
      */

    val groupManager10 = dactorSelection(classOf[GroupManager], 10)
    println(groupManager10)
    groupManager10 ! GroupManager.GetFixedDiscounts.Request(Seq(1,2,3))
  }

}
