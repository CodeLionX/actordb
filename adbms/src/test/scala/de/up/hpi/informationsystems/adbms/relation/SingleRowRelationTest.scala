package de.up.hpi.informationsystems.adbms.relation

import de.up.hpi.informationsystems.adbms.{IncompatibleColumnDefinitionException, RecordNotFoundException}
import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef, UntypedColumnDef}
import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.record.Record
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class SingleRowRelationTest extends WordSpec with Matchers {

  "A single row relation" should {

    object Customer extends RelationDef {
      val colFirstname: ColumnDef[String] = ColumnDef("Firstname")
      val colLastname: ColumnDef[String] = ColumnDef("Lastname")
      val colAge: ColumnDef[Int] = ColumnDef("Age")

      override val columns: Set[UntypedColumnDef] = Set(colFirstname, colLastname, colAge)
      override val name: String = "customer"
    }
    val record1 = Record(Customer.columns)
      .withCellContent(Customer.colFirstname)("Test")
      .withCellContent(Customer.colLastname)("Test")
      .withCellContent(Customer.colAge)(42)
      .build()

    val record2 = Record(Customer.columns)
      // missing firstName
      .withCellContent(Customer.colLastname)("Doe")
      .withCellContent(Customer.colAge)(200215)
      .build()

    val record3 = Record(Set(ColumnDef[String]("ID"), ColumnDef[Int]("LEVEL"))) // wrong record type
      .withCellContent(Customer.colLastname)("A8102C2")
      .withCellContent(Customer.colAge)(3)
      .build()

    "insert records without missing values correctly" in {
      val customer = SingleRowRelation(Customer)
      val inserted = customer.insert(record1)

      inserted shouldEqual Success(record1)
      customer.records shouldEqual Success(Seq(record1))
    }

    "insert records with missing values correctly" in {
      val customer = SingleRowRelation(Customer)
      val inserted = customer.insert(record2)

      inserted shouldEqual Success(record2)
      customer.records shouldEqual Success(Seq(record2))
    }

    "not allow inserting records with wrong columns" in {
      val customer = SingleRowRelation(Customer)
      val inserted = customer.insert(record3)

      inserted.isFailure shouldBe true
      inserted.failed.get match {
        case e: IncompatibleColumnDefinitionException =>
          e.getMessage.contains("the provided column layout does not match this relation's schema") shouldBe true
        case t => fail(s"the wrong exception was thrown\nexpected: IncompatibleColumnDefinitionException\nfound: $t")
      }
      customer.records shouldEqual Success(Seq.empty)
    }

    "not allow batch insert of records" in {
      val customer = SingleRowRelation(Customer)
      val inserted = customer.insertAll(Seq(record1, record2))

      inserted.isFailure shouldBe true
      inserted.failed.get match {
        case e: UnsupportedOperationException =>
          e.getMessage.contains("A single row relation can only contain one row!") shouldBe true
        case t => fail(s"the wrong exception was thrown\nexpected: UnsupportedOperationException\nfound: $t")
      }
      customer.records shouldEqual Success(Seq.empty)
    }

    "fail to insert records that do not adhere to the relations schema" in {
      val customer = SingleRowRelation(Customer)
      customer.insert(record3).isFailure shouldBe true
    }

    "allow deletion of records" in {
      val customer = SingleRowRelation(Customer)
      customer.insert(record1)
      val result = customer.delete(record1)

      result shouldEqual Success(record1)
      customer.records shouldEqual Success(Seq.empty)
    }

    "throw error when trying to delete non-existing record" in {
      val customer = SingleRowRelation(Customer)
      customer.insert(record1)
      val result = customer.delete(record2)

      result.isFailure shouldBe true
      result.failed.get match {
        case RecordNotFoundException(e) => e.contains("this relation does not contain the record") shouldBe true
        case t => fail(s"the wrong exception was thrown\nexpected: RecordNotFoundExcpetion\nfound: $t")
      }
      customer.records shouldBe Success(Seq(record1))
    }

    "allow inserting a new record after deletion" in {
      val customer = SingleRowRelation(Customer)
      customer.insert(record1)
      customer.delete(record1)
      val result = customer.insert(record2)

      result shouldEqual Success(record2)
      customer.records shouldEqual Success(Seq(record2))
    }

    "allow updating the record with simple where" in {
      val customer = SingleRowRelation(Customer)
      customer.insert(record1)

      val result = customer.update(Customer.colFirstname ~> "Hans" & Customer.colLastname ~> "Maier")
        .where[Int](Customer.colAge -> { _ == 42 })
      result shouldBe Success(1)

      customer.records shouldBe Success(Seq(
        Customer.newRecord(Customer.colAge ~> 42 & Customer.colFirstname ~> "Hans" & Customer.colLastname ~> "Maier").build()
      ))
    }

    "allow updating the record with multiple where conditions" in {
      val customer = SingleRowRelation(Customer)
      customer.insert(record1)

      val result = customer.update(Customer.colFirstname ~> "Hans" & Customer.colLastname ~> "Maier")
        .whereAll(
          Map(Customer.colAge.untyped -> ({ case age: Int => age == 42 }: Any => Boolean)) ++
          Map(Customer.colFirstname.untyped -> ({ case name: String => name == "Test" }: Any => Boolean))
        )
      result shouldBe Success(1)

      customer.records shouldBe Success(Seq(
        Customer.newRecord(Customer.colAge ~> 42 & Customer.colFirstname ~> "Hans" & Customer.colLastname ~> "Maier").build()
      ))
    }
  }
}
