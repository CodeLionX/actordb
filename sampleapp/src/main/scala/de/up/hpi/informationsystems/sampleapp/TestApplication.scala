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
  object UserRelationDefinition {
    val colFirstname: ColumnDef[String] = ColumnDef("Firstname")
    val colLastname: ColumnDef[String] = ColumnDef("Lastname")
    val colAge: ColumnDef[Int] = ColumnDef("Age")

    val R: ColumnRelation = ColumnRelation(Seq(colFirstname, colLastname, colAge))
  }

  /**
    * Definition of Columns and Relations for relation "Customer"
    */
  object CustomerRelationDefinition {
    val colCustomerId: ColumnDef[Int] = ColumnDef("Id")
    val colCustomerName: ColumnDef[String] = ColumnDef("Name")
    val colCustomerDiscount: ColumnDef[Double] = ColumnDef("Discount")

    val R: RowRelation = RowRelation(Seq(colCustomerId, colCustomerName, colCustomerDiscount))
  }

  import UserRelationDefinition._
  import CustomerRelationDefinition.{R => R2, colCustomerDiscount, colCustomerId, colCustomerName}

  /**
    * Testing User relation => ColumnStore
    */

  println(R.columns.mkString(", "))
  R.insert(
    Record(R.columns)
      .withCellContent(colLastname)("Maier")
      .withCellContent(colFirstname)("Hans")
      .withCellContent(colAge)(33)
      .build()
  )
  R.insert(
    Record(R.columns)
      .withCellContent(colFirstname)("Hans")
      .withCellContent(colLastname)("Schneider")
      .withCellContent(colAge)(12)
      .build()
  )
  R.insert(
    Record(R.columns)
      .withCellContent(colFirstname)("Justus")
      .withCellContent(colLastname)("Jonas")
      .build()
  )
  println()
  println(R)

  println()
  println("where:")
  println(
    R.where[String](colFirstname -> { _ == "Hans" }).pretty
  )
  println("whereAll:")
  println(
    R.whereAll(
      Map(colFirstname.untyped -> { name: Any => name.asInstanceOf[String] == "Hans" })
      ++ Map(colAge.untyped -> { age: Any => age.asInstanceOf[Int] == 33 })
    ).pretty
  )

  assert(ColumnDef[String]("Firstname") == colFirstname)
  assert(ColumnDef[Int]("Firstname").untyped != colFirstname.untyped)

  println()
  println("Projection of user relation:")
  println(R.project(Seq(colFirstname, colLastname)).getOrElse(Seq.empty).pretty)


  /**
    * Testing Customer relation => RowStore
    */

  println()
  println()
  val record = Record(R.columns)
    .withCellContent(colFirstname)("Hans")
    .withCellContent(colAge)(45)
    .withCellContent(colLastname)("")
    .build()
  println(record)
  assert(record.project(Seq(colAge)) == Success(Record(Seq(colAge)).withCellContent(colAge)(45).build()))
  assert(record.project(Seq(colAge, colCustomerDiscount)).isFailure)

  println(R2.columns.mkString(", "))
  println()
  R2.insert(
    Record(R2.columns)
      .withCellContent(colCustomerId)(1)
      .withCellContent(colCustomerName)("BMW")
      .build()
  )
  R2.insert(
    Record(R2.columns)
      .withCellContent(colCustomerId)(2)
      .withCellContent(colCustomerName)("BMW Group")
      .withCellContent(colCustomerDiscount)(0.023)
      .build()
  )
  R2.insert(
    Record(R2.columns)
      .withCellContent(colCustomerId)(3)
      .withCellContent(colCustomerName)("Audi AG")
      .withCellContent(colCustomerDiscount)(0.05)
      .build()
  )

  println(R2)
  println("where:")
  println(
    R2.where[String](colCustomerName -> { _.contains("BMW") }).pretty
  )

  val isBMW: Any => Boolean = value =>
    value.asInstanceOf[String].contains("BMW")

  def discountGreaterThan(threshold: Double): Any => Boolean = value =>
    value.asInstanceOf[Double] >= threshold

  println("whereAll:")
  println(
    R2.whereAll(
        Map(colCustomerName.untyped -> isBMW)
          ++ Map(colCustomerDiscount.untyped -> discountGreaterThan(0.01))
      )
      .pretty
  )

  println()
  println("Projection of customer relation:")
  println(R2.project(Seq(colCustomerName, colCustomerDiscount)).getOrElse(Seq.empty).pretty)
}
