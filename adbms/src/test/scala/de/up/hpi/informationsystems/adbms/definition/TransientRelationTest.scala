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

      "return an empty result set for any where query" in {
        emptyRelation
          .where(colFirstname, (_: String) => true)
          .records shouldEqual Success(Seq.empty)
      }

      "return an empty result set for any whereAll query" in {
        emptyRelation.whereAll(Map(
          colFirstname.untyped -> {_: Any => true},
          colAge.untyped -> {_: Any => true}
        )).records shouldEqual Success(Seq.empty)
      }

      "fail when joined with itself" in {
        emptyRelation
          .crossJoin(emptyRelation, (colFirstname, colFirstname))
          .records
          .isFailure shouldBe true
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
          .isFailure shouldBe true

        fullRelation
          .project(columns + ColumnDef[Int]("bad-col"))
          .records
          .isFailure shouldBe true
      }

      "fail when joined with an empty relation" in {
        fullRelation
          .crossJoin(TransientRelation(Seq.empty), (colFirstname, colFirstname))
          .records
          .isFailure shouldBe true
      }

      "return an appropriate result for join with itself with different columns" in {
        val diffColumnsJoined = fullRelation
          .crossJoin(fullRelation, (colFirstname, colLastname))

        diffColumnsJoined.columns shouldEqual columns
        diffColumnsJoined.records shouldEqual Success(Seq(record1))
      }

      "return itself for join with itself on same column" in {
        val sameColumnJoined = fullRelation
          .crossJoin(fullRelation, (colFirstname, colFirstname))

        sameColumnJoined.columns shouldEqual columns
        sameColumnJoined.records shouldEqual Success(Seq(record1, record2))
      }

      "return appropriate result for join" in {
        val colFirstname2 = ColumnDef[String]("Firstname2")
        val col1 = ColumnDef[Double]("col1")

        val otherRecord1 = Record(Set(colFirstname2, col1))
          .withCellContent(colFirstname2)("Test")
          .withCellContent(col1)(12.1)
          .build()

        val otherRecord2 = Record(Set(colFirstname2, col1))
          .withCellContent(colFirstname2)("Test")
          .withCellContent(col1)(916.93)
          .build()

        val otherRel = TransientRelation(Seq(otherRecord1, otherRecord2))
        val sameColumnJoined = fullRelation
          .crossJoin(otherRel, (colFirstname, colFirstname2))

        sameColumnJoined.columns shouldEqual columns + colFirstname2 + col1
        sameColumnJoined.records shouldEqual Success(Seq(
          record1 ++ otherRecord1,
          record1 ++ otherRecord2
        ))
      }

      "fail to join on wrong column definition" in {
        val joined1 = fullRelation
          .crossJoin(fullRelation, (ColumnDef[String]("something"), colFirstname))
          .records
        val joined2 = fullRelation
          .crossJoin(fullRelation, (colFirstname, ColumnDef[String]("something")))
          .records

        joined1.isFailure shouldBe true
        joined2.isFailure shouldBe true
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
