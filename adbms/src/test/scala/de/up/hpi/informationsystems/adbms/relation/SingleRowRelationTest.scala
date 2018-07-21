package de.up.hpi.informationsystems.adbms.relation

import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef}
import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.{IncompatibleColumnDefinitionException, RecordNotFoundException}
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class SingleRowRelationTest extends WordSpec with Matchers {

  "A single row relation" should {

    object Customer extends RelationDef {
      val colFirstname: ColumnDef[String] = ColumnDef[String]("Firstname")
      val colLastname: ColumnDef[String] = ColumnDef[String]("Lastname")
      val colAge: ColumnDef[Int] = ColumnDef[Int]("Age")

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
      inserted.failed match {
        case Success(e: IncompatibleColumnDefinitionException) =>
          e.getMessage.contains("the provided column layout does not match this relation's schema") shouldBe true
        case Success(t) =>
          fail(s"the wrong exception was thrown\nexpected: IncompatibleColumnDefinitionException\nfound: $t")
        case _ => fail("unexpected match!")
      }
      customer.records shouldEqual Success(Seq.empty)
    }

    "not allow batch insert of records" in {
      val customer = SingleRowRelation(Customer)
      val inserted = customer.insertAll(Seq(record1, record2))

      inserted.isFailure shouldBe true
      inserted.failed match {
        case Success(e: UnsupportedOperationException) =>
          e.getMessage.contains("A single row relation can only contain one row!") shouldBe true
        case Success(t) =>
          fail(s"the wrong exception was thrown\nexpected: UnsupportedOperationException\nfound: $t")
        case _ => fail("unexpected match!")
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
      result.failed match {
        case Success(RecordNotFoundException(e)) =>
          e.contains("this relation does not contain") shouldBe true
        case Success(t) =>
          fail(s"the wrong exception was thrown\nexpected: RecordNotFoundExcpetion\nfound: $t")
        case _ => fail("unexpected match!")
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
          Map(Customer.colAge -> ({ case age: Int => age == 42 }: Any => Boolean)) ++
          Map(Customer.colFirstname -> ({ case name: String => name == "Test" }: Any => Boolean))
        )
      result shouldBe Success(1)

      customer.records shouldBe Success(Seq(
        Customer.newRecord(Customer.colAge ~> 42 & Customer.colFirstname ~> "Hans" & Customer.colLastname ~> "Maier").build()
      ))
    }

    "fail on where query with wrong column definition" in {
      val customer = SingleRowRelation(Customer)
      customer.insert(record1)

      val result = customer.where(ColumnDef[Double]("WRONG") -> { d: Double => d == 2.1 }).records

      result.isFailure shouldBe true
    }

    "fail on whereAll query with wrong column definition" in {
      val customer = SingleRowRelation(Customer)
      customer.insert(record1)

      val result = customer.whereAll(
        Map(ColumnDef[Double]("WRONG") -> { case d: Double => d == 2.1 })
      ).records

      result.isFailure shouldBe true
    }

    "fail on project query with wrong column definition" in {
      val customer = SingleRowRelation(Customer)
      customer.insert(record1)

      val result = customer.project(
        Set(ColumnDef[Double]("WRONG"))
      ).records

      result.isFailure shouldBe true
    }

    "return empty result on all unary operations when empty" in {
      val customer = SingleRowRelation(Customer)

      val result1 = customer.project(Set(Customer.colFirstname)).records
      val result2 = customer.where(Customer.colFirstname -> { s: String => s == "" }).records
      val result3 = customer.whereAll(Map(Customer.colFirstname -> { case s: String => s == "" })).records
      val result4 = customer.applyOn(Customer.colFirstname, { s: String => s"$s test" }).records

      result1 shouldEqual Success(Seq.empty)
      result2 shouldEqual Success(Seq.empty)
      result3 shouldEqual Success(Seq.empty)
      result4 shouldEqual Success(Seq.empty)
    }
  }
}
