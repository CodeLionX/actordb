package de.up.hpi.informationsystems.adbms.definition

import org.scalatest.{Matchers, WordSpec}

import scala.util.{Success, Failure}

class RecordTest extends WordSpec with Matchers {

  "A record builder" should {
    val col1 = ColumnDef[String]("col1")
    val col2 = ColumnDef[Int]("col2")
    val col3 = ColumnDef[Double]("col3")
    val val1 = "val1"
    val val2 = 2
    val val3 = 3.0

    "build a record correctly with and without implicit" in {
      val builder = Record(Set(col1, col2, col3))
      val r1 = builder
        .withCellContent(col1)(val1)
        .withCellContent(col2)(val2)
        .withCellContent(col3)(val3)
        .build()

      import Record.implicits._
      val r2 = builder(
        col1 ~> val1 &
        col2 ~> val2 and
        col3 ~> val3 +
        col3 ~> val3
      ).build()

      r1 shouldEqual r2
    }

    "fail to compile if types of column and value do not match in withCellContent" in {
      assertTypeError("val r = Record(Seq(col1, col2, col3)).withCellContent(col1)(val2).build()")
      assertTypeError("val r = Record(Seq(col1, col2, col3)).withCellContent(col2)(val3).build()")
      assertTypeError("val r = Record(Seq(col1, col2, col3)).withCellContent(col3)(val1).build()")
    }

    "fail to compile if types of column and value do not match with apply and RecordBuilderPart" in {
      import Record.implicits._
      assertTypeError("val r = Record(Seq(col1, col2, col3))(col1 ~> val2).build()")
      assertTypeError("val r = Record(Seq(col1, col2, col3))(col2 ~> val3).build()")
      assertTypeError("val r = Record(Seq(col1, col2, col3))(col3 ~> val1).build()")
    }
  }

  "A record" when {
    "empty" should {
      val emptyRecord = Record(Set.empty).build()

      "have an empty column list" in {
        emptyRecord.columns shouldBe empty
      }

      "not return any data" in {
        emptyRecord.get(ColumnDef[Any]("")) shouldBe empty
      }

      "project to itself, when projected by an empty list" in {
        emptyRecord.project(Set.empty) should equal(Success(emptyRecord))
      }

      "not allow projection, when projecting by any column definition" in {
        emptyRecord.project(Set(ColumnDef[Any](""))).isFailure shouldBe true
        emptyRecord.project(new RowRelation {
          override def columns: Set[UntypedColumnDef] = Set(ColumnDef[Any](""))
        }.columns).isFailure shouldBe true
      }
    }

    "empty with column definitions" should {
      val col1 = ColumnDef[String]("col1")
      val col2 = ColumnDef[Int]("col2")
      val col3 = ColumnDef[Double]("col3")
      val record = Record(Set(col1, col2, col3)).build()
      val R = new RowRelation {
        override def columns: Set[UntypedColumnDef] = Set(col1, col2)
      }

      "have a column set" in {
        record.columns.size shouldBe 3
        record.columns shouldEqual Set(col1, col2, col3)
      }

      "return None, when accessing any column" in {
        record.get(ColumnDef[Any]("")) shouldBe None
        record.get(col1) shouldBe None
        record.get(col2) shouldBe None
        record.get(col3) shouldBe None
      }

      "project to empty record, when projected by an empty list" in {
        record.project(Set.empty) should equal(Success(Record(Set.empty).build()))
      }

      "not allow projection, when projecting by any column definition" in {
        record.project(Set(ColumnDef[Any](""))) should be.leftSideValue
      }

      "project to correct column subset, when projecting by contained columns" in {
        record.project(Set(col1)) should equal(Success(Record(Set(col1)).build()))
        record.project(Set(col2)) should equal(Success(Record(Set(col2)).build()))
        record.project(Set(col1, col3)) should equal(Success(Record(Set(col1, col3)).build()))
        record.project(R.columns) should equal(Success(Record(Set(col1, col2)).build()))
      }
    }

    "full" should {
      val col1 = ColumnDef[String]("col1")
      val col2 = ColumnDef[Int]("col2")
      val col3 = ColumnDef[Double]("col3")
      val val1 = "val1"
      val val2 = 2
      val val3 = 3.0
      val record = Record(Set(col1, col2, col3))
        .withCellContent(col1)(val1)
        .withCellContent(col2)(val2)
        .withCellContent(col3)(val3)
        .build()
      val R = new RowRelation {
        override def columns: Set[UntypedColumnDef] = Set(col1, col2)
      }

      "return the column's cell value" in {
        record.get(ColumnDef[Any]("")) shouldBe None
        record.get(col1) shouldBe Some(val1)
        record.get(col2) shouldBe Some(val2)
        record.get(col3) shouldBe Some(val3)
      }

      "retain values of projected columns and drop others" in {
        record.project(Set(col1)) match {
          case Success(r) =>
            r.get(col1) shouldBe Some(val1)
            r.get(col2) shouldBe None
            r.get(col3) shouldBe None
          case Failure(_) => fail("Projection by column sequence failed, but it should succeed")
        }
        record.project(R.columns) match {
          case Success(r) =>
            r.get(col1) shouldBe Some(val1)
            r.get(col2) shouldBe Some(val2)
            r.get(col3) shouldBe None
          case Failure(_) => fail("Projection by Relation failed, but it should succeed")
        }
      }
    }
  }

}
