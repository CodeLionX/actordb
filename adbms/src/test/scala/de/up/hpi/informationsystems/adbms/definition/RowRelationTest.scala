package de.up.hpi.informationsystems.adbms.definition

import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class RowRelationTest extends WordSpec with Matchers {

  "A row relation" should {

    object Customer extends RowRelation {
      val colFirstname: ColumnDef[String] = ColumnDef("Firstname")
      val colLastname: ColumnDef[String] = ColumnDef("Lastname")
      val colAge: ColumnDef[Int] = ColumnDef("Age")

      override val columns: Set[UntypedColumnDef] = Set(colFirstname, colLastname, colAge)
    }

    val record1 = Record(Customer.columns)
      .withCellContent(Customer.colFirstname)("Test")
      .withCellContent(Customer.colLastname)("Test")
      .withCellContent(Customer.colAge)(42)
      .build()

    val record2 = Record(Set(Customer.colFirstname, Customer.colLastname, Customer.colAge))
      .withCellContent(Customer.colFirstname)("Max")
      .withCellContent(Customer.colLastname)("Mustermann")
      .withCellContent(Customer.colAge)(23)
      .build()

    val record3 = Record(Customer.columns)
      // missing firstName
      .withCellContent(Customer.colLastname)("Doe")
      .withCellContent(Customer.colAge)(200215)
      .build()

    val record4 = Record(Set(ColumnDef[String]("ID"), ColumnDef[Int]("LEVEL"))) // wrong record type
      .withCellContent(Customer.colLastname)("A8102C2")
      .withCellContent(Customer.colAge)(3)
      .build()

    "insert records with and without missing values correctly" in {
      val inserted1 = Customer.insert(record1)
      val inserted2 = Customer.insert(record2)
      val inserted3 = Customer.insert(record3)

      inserted1 should equal (Success(record1))
      inserted2 should equal (Success(record2))
      inserted3 should equal (Success(record3))
    }

    "allow for batch insert of records with and without missing values correctly" in {
      val inserted = Customer.insertAll(Seq(record1, record2, record3))
      inserted should equal (Success(Seq(record1, record2, record3)))
    }

    "fail to insert records that do not adhere to the relations schema" in {
      Customer.insert(record4).isFailure should equal (true)
    }

    "fail to batch insert records with at least one that does not adhere to the relations schema" in {
      Customer.insertAll(Seq(record1, record2, record3, record4)).isFailure should equal (true)
    }
  }

  "A row relation" when {

    "empty" should {

      object TestRelation extends RowRelation {
        val colId = ColumnDef[Int]("id")
        val colField = ColumnDef[String]("field")

        override val columns: Set[UntypedColumnDef] = Set(colId, colField)
      }

      "return an empty result set for any where or whereAll query" in {
        TestRelation.where(TestRelation.colId, (_: Int) => true) shouldEqual Seq()
        TestRelation.whereAll(Map(
          TestRelation.colId.untyped -> {_: Any => true},
          TestRelation.colField.untyped -> {_: Any => true}
        )) shouldEqual Seq()
      }
    }

    "full" should {

      object TestRelation extends RowRelation {
        val colId = ColumnDef[Int]("id")
        val colField = ColumnDef[String]("field")

        override val columns: Set[UntypedColumnDef] = Set(colId, colField)
      }

      val rec1 = Record(TestRelation.columns)
        .withCellContent(TestRelation.colId)(1)
        .withCellContent(TestRelation.colField)("Testy")
        .build()

      val rec2 = Record(TestRelation.columns)
        .withCellContent(TestRelation.colId)(2)
        .withCellContent(TestRelation.colField)("Foo")
        .build()

      val rec3 = Record(TestRelation.columns)
        .withCellContent(TestRelation.colId)(3)
        .withCellContent(TestRelation.colField)("Bar")
        .build()

      TestRelation.insertAll(
        Seq(rec1, rec2, rec3)
      )

      "return the appropriate result set for a where query including the empty result set" in {
        TestRelation.where(TestRelation.colId, (_: Int) => true) shouldEqual Seq(rec1, rec2, rec3)
        TestRelation.where(TestRelation.colId, (id: Int) => id >= 2) shouldEqual Seq(rec2, rec3)
        TestRelation.where(TestRelation.colId, (id: Int) => id >= 5) shouldEqual Seq()
      }

      "return the appropriate result set for a whereAll query including the empty result set" in {
        TestRelation.whereAll(
          Map(
            TestRelation.colId.untyped -> {id: Any => id.asInstanceOf[Int] <= 2},
            TestRelation.colField.untyped -> {field: Any => field.asInstanceOf[String].contains("esty")}
          )) shouldEqual Seq(rec1)
      }

      "return selected columns only from project" in {
        // deduce correctness of Relation.project from correctness of Record.project
        TestRelation.project(Set(TestRelation.colField)).records.get shouldEqual
          Seq(
            rec1.project(Set(TestRelation.colField)).get,
            rec2.project(Set(TestRelation.colField)).get,
            rec3.project(Set(TestRelation.colField)).get
          )
      }

      "fail to project to non-existent columns" in {
        TestRelation.project(Set(ColumnDef[Int]("bad-col"))).records.isFailure should equal (true)
        TestRelation.project(TestRelation.columns + ColumnDef[Int]("bad-col")).records.isFailure should equal (true)
      }
    }

    "filled with incomplete records, i.e. missing values" should {
      object TestRelation extends RowRelation {
        val colId = ColumnDef[Int]("id")
        val colField = ColumnDef[String]("field")

        override val columns: Set[UntypedColumnDef] = Set(colId, colField)
      }

      val rec1 = Record(TestRelation.columns)
        .withCellContent(TestRelation.colId)(1)
        .withCellContent(TestRelation.colField)("Testy")
        .build()

      val rec2 = Record(TestRelation.columns)
        .withCellContent(TestRelation.colId)(2)
        .withCellContent(TestRelation.colField)(null)
        .build()

      val rec3 = Record(TestRelation.columns)
        .withCellContent(TestRelation.colId)(3)
        .build()

      TestRelation.insertAll(
        Seq(rec1, rec2, rec3)
      )

      "return the appropriate result set for a where query including the empty result set" in {
        TestRelation.where(TestRelation.colId, (_: Any) => true) shouldEqual Seq(rec1, rec2, rec3)
        TestRelation.where(TestRelation.colId, (id: Int) => id <= 2) shouldEqual Seq(rec1, rec2)
        TestRelation.where(TestRelation.colId, (id: Int) => id >= 5) shouldEqual Seq()
        TestRelation.where(TestRelation.colField, (field: String) => field.contains("esty")) shouldEqual Seq(rec1)
      }

      "return the appropriate result set for a whereAll query including the empty result set" ignore {
        TestRelation.whereAll(
          Map(
            TestRelation.colId.untyped -> {id: Any => id.asInstanceOf[Int] <= 2},
            TestRelation.colField.untyped -> {field: Any => field.asInstanceOf[String].contains("esty")}
          )) shouldEqual Seq(rec1)
      }
    }
  }

}
