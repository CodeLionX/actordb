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
        emptyRecord.project(Seq(ColumnDef[Any](""))).isLeft shouldBe true
        emptyRecord.project(RowRelation(Seq(ColumnDef[Any]("")))).isLeft shouldBe true
      }
    }

    "empty with column definitions" should {
      val col1 = ColumnDef[String]("col1")
      val col2 = ColumnDef[Int]("col2")
      val col3 = ColumnDef[Double]("col3")
      val record = Record(Seq(col1, col2, col3)).build()
      val R = RowRelation(Seq(col1, col2))

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
        record.project(R) should equal(Right(Record(Seq(col1, col2)).build()))
      }
    }

    "full" should {
      val col1 = ColumnDef[String]("col1")
      val col2 = ColumnDef[Int]("col2")
      val col3 = ColumnDef[Double]("col3")
      val val1 = "val1"
      val val2 = 2
      val val3 = 3.0
      val record = Record(Seq(col1, col2, col3))
        .withCellContent(col1 -> val1)
        .withCellContent(col2 -> val2)
        .withCellContent(col3 -> val3)
        .build()
      val R = RowRelation(Seq(col1, col2))

      "return the column's cell value" in {
        record.get(ColumnDef[Any]("")) shouldBe None
        record.get(col1) shouldBe Some(val1)
        record.get(col2) shouldBe Some(val2)
        record.get(col3) shouldBe Some(val3)
      }

      "retain values of projected columns and drop others" in {
        record.project(Seq(col1)) match {
          case Right(r) =>
            r.get(col1) shouldBe Some(val1)
            r.get(col2) shouldBe None
            r.get(col3) shouldBe None
          case Left(_) => fail("Projection by column sequence failed, but it should succeed")
        }
        record.project(R) match {
          case Right(r) =>
            r.get(col1) shouldBe Some(val1)
            r.get(col2) shouldBe Some(val2)
            r.get(col3) shouldBe None
          case Left(_) => fail("Projection by Relation failed, but it should succeed")
        }
      }
    }
  }

}
