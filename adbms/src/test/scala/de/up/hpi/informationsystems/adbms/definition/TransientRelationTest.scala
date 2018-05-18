package de.up.hpi.informationsystems.adbms.definition

import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class TransientRelationTest extends WordSpec with Matchers {

  "A transient relation" when {

    val colFirstname: ColumnDef[String] = ColumnDef("Firstname")
    val colLastname: ColumnDef[String] = ColumnDef("Lastname")
    val colAge: ColumnDef[Int] = ColumnDef("Age")
    
    val columns: Set[UntypedColumnDef] = Set(colFirstname, colLastname, colAge)

    val record1 = Record(columns)
      .withCellContent(colFirstname)("Test")
      .withCellContent(colLastname)("Test")
      .withCellContent(colAge)(42)
      .build()

    val record2 = Record(Set(colFirstname, colLastname, colAge))
      .withCellContent(colFirstname)("Max")
      .withCellContent(colLastname)("Mustermann")
      .withCellContent(colAge)(23)
      .build()

    val record3 = Record(columns)
      // missing firstName
      .withCellContent(colLastname)("es")
      .withCellContent(colAge)(200215)
      .build()

    val record4 = Record(columns)
      .withCellContent(colFirstname)(null)
      .withCellContent(colAge)(2)
      .build()

    "empty" should {
      val emptyRelation = TransientRelation(Seq.empty)

      "return an empty result set for any where or whereAll query" in {
        emptyRelation
          .where(colFirstname, (_: String) => true)
          .records shouldEqual Success(Seq.empty)

        emptyRelation.whereAll(Map(
          colFirstname.untyped -> {_: Any => true},
          colAge.untyped -> {_: Any => true}
        )).records shouldEqual Success(Seq.empty)
      }
    }

    "full" should {
      val fullRelation = TransientRelation(Seq(record1, record2))

      "return the appropriate result set for a where query including the empty result set" in {
        fullRelation
          .where(colFirstname, (_: String) => true)
          .records shouldEqual Success(Seq(record1, record2))

        fullRelation
          .where(colAge, (id: Int) => id == 23)
          .records shouldEqual Success(Seq(record2))

        fullRelation
          .where(colAge, (id: Int) => id > 42)
          .records shouldEqual Success(Seq.empty)
      }

      "return the appropriate result set for a whereAll query including the empty result set" in {
        fullRelation.whereAll(Map(
            colAge.untyped -> {id: Any => id.asInstanceOf[Int] <= 23},
            colFirstname.untyped -> {field: Any => field.asInstanceOf[String].contains("Max")}
        )).records shouldEqual Success(Seq(record2))
      }

      "return selected columns only from project" in {
        // deduce correctness of Relation.project from correctness of Record.project
        fullRelation.project(Set(colFirstname)).records shouldEqual
          Success(Seq(
            record1.project(Set(colFirstname)).get,
            record2.project(Set(colFirstname)).get
          ))
      }

      "fail to project to non-existent columns" in {
        fullRelation
          .project(Set(ColumnDef[Int]("bad-col")))
          .records
          .isFailure should equal (true)

        fullRelation
          .project(columns + ColumnDef[Int]("bad-col"))
          .records
          .isFailure should equal (true)
      }
    }

    "filled with incomplete records, i.e. missing values" should {
      val incompleteRelation = TransientRelation(Seq(record1, record2, record3, record4))

      "return the appropriate result set for a where query" when {

        "matching all" in {
          incompleteRelation
            .where(colAge, (_: Any) => true)
            .records shouldEqual Success(Seq(record1, record2, record3, record4))
        }

        "smaller result set" in {
          incompleteRelation
            .where(colAge, (id: Int) => id >= 42)
            .records shouldEqual Success(Seq(record1, record3))
        }

        "empty result set" in {
          incompleteRelation
            .where(colAge, (id: Int) => id < 2)
            .records shouldEqual Success(Seq.empty)
        }

        "condition on column with null-value" in {
          incompleteRelation
            .where(colFirstname, (field: String) => field.contains("Test"))
            .records shouldEqual Success(Seq(record1))
        }
      }

      "return the appropriate result set for a whereAll query including the empty result set" in {
        pending // NullPointerException
        incompleteRelation.whereAll(
          Map(
            colAge.untyped -> {age: Any => age.asInstanceOf[Int] > 40},
            colLastname.untyped -> {field: Any => field.asInstanceOf[String].contains("es")}
          )).records shouldEqual Success(Seq(record1, record3))
        }

      "return selected columns only from project including records with null-values" in {
        pending // NullPointerException
        // deduce correctness of Relation.project from correctness of Record.project
        incompleteRelation.project(Set(colFirstname)).records shouldEqual
          Success(Seq(
            record1.project(Set(colFirstname)).get,
            record2.project(Set(colFirstname)).get,
            record3.project(Set(colFirstname)).get
          ))
      }

      "return the appropriate result set for a whereAll query including condition on null-value column" in {
        pending // NullPointerException
        incompleteRelation.whereAll(
          Map(
            colAge.untyped -> {id: Any => id.asInstanceOf[Int] <= 2},
            colFirstname.untyped -> {field: Any => field.asInstanceOf[String].contains("esty")}
          )).records shouldEqual Success(Seq.empty)
      }
    }
  }
}
