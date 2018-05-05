package de.up.hpi.informationsystems.sampleapp

import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, ColumnRelation, TypedColumnDef}

object TestApplication extends App {

  /**
    * Definition of Columns and Relations
    */
  object Definition {
    val colFirstname: TypedColumnDef[String] = ColumnDef[String]("Firstname")
    val colAge: TypedColumnDef[Int] = ColumnDef[Int]("Age")

    val relation: ColumnRelation = ColumnRelation(Seq(colAge, colFirstname))
  }
  import Definition._

  println(relation.columns.mkString(", "))
  relation.insert(colFirstname, "Sebastian Schmidl")
  relation.insert(colFirstname, "Frederic Schneider")
  relation.insert(colAge, 23)
  relation.insert(colAge, 24)
  relation.insert(colFirstname, "Marcel Weisgut")

  println(relation)

  assert(ColumnDef[String]("Firstname") == colFirstname)
  assert(ColumnDef[Int]("Firstname").asInstanceOf[ColumnDef] != colFirstname.asInstanceOf[ColumnDef])
}
