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

  // queries on data from RowRelation are performed in TransientRelation and therefore tested in its test suite

}
