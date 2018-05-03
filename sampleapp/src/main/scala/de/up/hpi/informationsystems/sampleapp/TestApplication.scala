package de.up.hpi.informationsystems.sampleapp

import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, ColumnRelation}

object TestApplication extends App {
  val colFirstname  = ColumnDef[String]("Firstname")
  val colAge        = ColumnDef[Int]("Age")

  val relation = ColumnRelation(Seq(colAge, colFirstname))

  println(relation.columns.mkString(", "))
  relation.insert(colFirstname, "Sebastian Schmidl")
  relation.insert(colFirstname, "Frederic Schneider")
  relation.insert(colAge, 23)
  relation.insert(colAge, 24)
  relation.insert(colFirstname, "Marcel Weisgut")

  println(relation)

  assert(ColumnDef[String]("Firstname") == colFirstname)
  // fails, but should hold:
  // FIXME: change TypedColumnDef from case class to class and implement equals on our own
  assert(ColumnDef[Int]("Firstname").asInstanceOf[ColumnDef] != colFirstname.asInstanceOf[ColumnDef])
}
