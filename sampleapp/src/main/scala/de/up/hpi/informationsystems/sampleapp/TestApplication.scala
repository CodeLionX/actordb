package de.up.hpi.informationsystems.sampleapp

import de.up.hpi.informationsystems.adbms.definition._

object TestApplication extends App {

  /**
    * Definition of Columns and Relations for relation "User"
    */
  object UserRelationDefinition {
    val colFirstname: TypedColumnDef[String] = ColumnDef("Firstname")
    val colLastname: TypedColumnDef[String] = ColumnDef("Lastname")
    val colAge: TypedColumnDef[Int] = ColumnDef("Age")

    val R: ColumnRelation = ColumnRelation(Seq(colAge, colFirstname))
  }

  /**
    * Definition of Columns and Relations for relation "Customer"
    */
  object CustomerRelationDefinition {
    val colCustomerId: TypedColumnDef[Int] = ColumnDef("Id")
    val colCustomerName: TypedColumnDef[String] = ColumnDef("Name")
    val colCustomerDiscount: TypedColumnDef[Double] = ColumnDef("Discount")

    val R: RowRelation = RowRelation(Seq(colCustomerId, colCustomerName, colCustomerDiscount))
  }

  import UserRelationDefinition._
  import CustomerRelationDefinition.{R => R2, colCustomerDiscount, colCustomerId, colCustomerName}

  println(R.columns.mkString(", "))
  R.insert(colFirstname, "Sebastian Schmidl")
  R.insert(colFirstname, "Frederic Schneider")
  R.insert(colAge, 23)
  R.insert(colAge, 24)
  R.insert(colFirstname, "Marcel Weisgut")

  println()
  println(R)

  assert(ColumnDef[String]("Firstname") == colFirstname)
  assert(ColumnDef[Int]("Firstname").asInstanceOf[ColumnDef] != colFirstname.asInstanceOf[ColumnDef])


  val record = Record(R.columns)
    .withCellContent(colFirstname -> "Hans")
    .withCellContent(colAge -> 45)
    .withCellContent(colLastname -> "")
    .build()
  println(record)

  println(R2.columns.mkString(", "))
  println()
  R2.insert(
    Record(R2.columns)
      .withCellContent(colCustomerId -> 1)
      .withCellContent(colCustomerName -> "BMW")
      .build()
  )
  R2.insert(
    Record(R2.columns)
      .withCellContent(colCustomerId -> 2)
      .withCellContent(colCustomerName -> "BMW Group")
      .withCellContent(colCustomerDiscount -> 0.023)
      .build()
  )
  R2.insert(
    Record(R2.columns)
      .withCellContent(colCustomerId -> 3)
      .withCellContent(colCustomerName -> "Audi AG")
      .withCellContent(colCustomerDiscount -> 0.05)
      .build()
  )

  println(R2)
  println("where:")
  println(
    R2.where[String](colCustomerName -> { _.contains("BMW") })
      .map{ _.values.mkString(", ") }
      .mkString("\n")
  )

  val isBMW: Any => Boolean = value =>
    value.asInstanceOf[String].contains("BMW")

  def discountGreaterThan(threshold: Double): Any => Boolean = value =>
    value.asInstanceOf[Double] >= threshold

  println("whereAll:")
  println(
    R2.whereAll(
        Map(colCustomerName.untyped -> isBMW)
//          ++ Map(colCustomerId.untyped -> { value: Any => value.asInstanceOf[Int] == 1 })
          ++ Map(colCustomerDiscount.untyped -> discountGreaterThan(0.01))
      )
      .map{ _.values.mkString(", ") }
      .mkString("\n")
  )
}
