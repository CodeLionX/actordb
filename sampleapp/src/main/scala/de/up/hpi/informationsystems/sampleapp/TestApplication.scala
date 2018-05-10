package de.up.hpi.informationsystems.sampleapp

import de.up.hpi.informationsystems.adbms.definition._

import scala.util.Success

object TestApplication extends App {

  implicit class RecordSeqToString(a: Seq[Record]) {
    def pretty: String =
      a.map{ _.values.mkString(", ") }
        .mkString("\n")
  }

  /**
    * Definition of Columns and Relations for relation "User"
    */
  object User extends ColumnRelation {
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
    User.where[String](User.colFirstname -> { _ == "Hans" }).pretty
  )
  println("whereAll:")
  println(
    User.whereAll(
      Map(User.colFirstname.untyped -> { name: Any => name.asInstanceOf[String] == "Hans" })
      ++ Map(User.colAge.untyped -> { age: Any => age.asInstanceOf[Int] == 33 })
    ).pretty
  )

  assert(ColumnDef[String]("Firstname") == User.colFirstname)
  assert(ColumnDef[Int]("Firstname").untyped != User.colFirstname.untyped)

  println()
  println("Projection of user relation:")
  println(User.project(Set(User.colFirstname, User.colLastname): Set[UntypedColumnDef]).getOrElse(Set.empty))


  /**
    * Testing Customer relation => RowStore
    */

  println()
  println()
  import Record.implicits._
  val record = Record(User.columns)(
      User.colFirstname ~> "Hans" &
      User.colLastname ~> "Lastname" &
      User.colAge ~> 45
    )
    .build()
  println(record)
  assert(record.project(Set(User.colAge): Set[UntypedColumnDef]) == Success(Record(Set(User.colAge))(User.colAge ~> 45).build()))
  assert(record.project(Set(User.colAge, Customer.colDiscount): Set[UntypedColumnDef]).isFailure)

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
    Customer.where[String](Customer.colName -> { _.contains("BMW") }).pretty
  )

  val isBMW: Any => Boolean = value =>
    value.asInstanceOf[String].contains("BMW")

  def discountGreaterThan(threshold: Double): Any => Boolean = value =>
    value.asInstanceOf[Double] >= threshold

  println("whereAll:")
  println(
    Customer.whereAll(
        Map(Customer.colName.untyped -> isBMW)
          ++ Map(Customer.colDiscount.untyped -> discountGreaterThan(0.01))
      )
      .pretty
  )

  println()
  println("Projection of customer relation:")
  println(Customer.project(Set(Customer.colName, Customer.colDiscount): Set[UntypedColumnDef]).getOrElse(Set.empty))
}
