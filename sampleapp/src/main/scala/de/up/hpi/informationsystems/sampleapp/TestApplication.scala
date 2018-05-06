package de.up.hpi.informationsystems.sampleapp

import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, ColumnRelation, Record, TypedColumnDef}

object TestApplication extends App {

  /**
    * Definition of Columns and Relations
    */
  object UserRelationDefinition {
    val colFirstname: TypedColumnDef[String] = ColumnDef[String]("Firstname")
    val colLastname: TypedColumnDef[String] = ColumnDef[String]("Lastname")
    val colAge: TypedColumnDef[Int] = ColumnDef[Int]("Age")

    val R: ColumnRelation = ColumnRelation(Seq(colAge, colFirstname))
  }
  import UserRelationDefinition._

  println(R.columns.mkString(", "))
  R.insert(colFirstname, "Sebastian Schmidl")
  R.insert(colFirstname, "Frederic Schneider")
  R.insert(colAge, 23)
  R.insert(colAge, 24)
  R.insert(colFirstname, "Marcel Weisgut")

  println(R)

  assert(ColumnDef[String]("Firstname") == colFirstname)
  assert(ColumnDef[Int]("Firstname").asInstanceOf[ColumnDef] != colFirstname.asInstanceOf[ColumnDef])


  val record = Record(R.columns)
    .withCellContent(colFirstname -> "Hans")
    .withCellContent(colAge -> 45)
    .withCellContent(colLastname -> "")
    .build()
  println(record)
}
