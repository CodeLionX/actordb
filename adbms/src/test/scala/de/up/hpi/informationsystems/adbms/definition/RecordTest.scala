package de.up.hpi.informationsystems.adbms.definition

import org.scalatest.{Matchers, WordSpec}

class RecordTest extends WordSpec with Matchers {

  "A record" when {
    "empty" should {
      val emptyRecord = Record(Seq.empty).build()

      "have an empty column list" in {
        emptyRecord.columns shouldBe empty
      }

      "not return any data" in {
        emptyRecord.get(ColumnDef[Any]("")) shouldBe empty
      }

      "project to itself, when projected by an empty list" in {
        emptyRecord.project(Seq.empty) should equal(Right(emptyRecord))
      }

      "not allow projection, when projecting by any column definition" in {
        emptyRecord.project(Seq(ColumnDef[Any](""))) should be.leftSideValue
      }
    }

    "empty with column definitions" should {
      val col1 = ColumnDef[String]("col1")
      val col2 = ColumnDef[Int]("col2")
      val col3 = ColumnDef[Double]("col3")
      val record = Record(Seq(col1, col2, col3)).build()

      "have a column list" in {
        record.columns.size shouldBe 3
        record.columns shouldEqual Seq(col1, col2, col3)
      }

      "return None, when accessing any column" in {
        record.get(ColumnDef[Any]("")) shouldBe None
        record.get(col1) shouldBe None
        record.get(col2) shouldBe None
        record.get(col3) shouldBe None
      }

      "project to empty record, when projected by an empty list" in {
        record.project(Seq.empty) should equal(Right(Record(Seq.empty).build()))
      }

      "not allow projection, when projecting by any column definition" in {
        record.project(Seq(ColumnDef[Any](""))) should be.leftSideValue
      }

      "project to correct column subset, when projecting by contained columns" in {
        record.project(Seq(col1)) should equal(Right(Record(Seq(col1)).build()))
        record.project(Seq(col2)) should equal(Right(Record(Seq(col2)).build()))
        record.project(Seq(col1, col3)) should equal(Right(Record(Seq(col1, col3)).build()))
      }
    }
  }

}
