package de.up.hpi.informationsystems.adbms.relation

import de.up.hpi.informationsystems.adbms.RecordNotFoundException
import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef}
import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.record.Record
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class RowRelationTest extends WordSpec with Matchers {

  "A row relation" should {

    object Customer extends RelationDef {
      val colFirstname: ColumnDef[String] = ColumnDef[String]("Firstname")
      val colLastname: ColumnDef[String] = ColumnDef[String]("Lastname")
      val colAge: ColumnDef[Int] = ColumnDef[Int]("Age")

      override val columns: Set[UntypedColumnDef] = Set(colFirstname, colLastname, colAge)
      override val name: String = "customer"
    }
    val customer = RowRelation(Customer)

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
      val inserted1 = customer.insert(record1)
      val inserted2 = customer.insert(record2)
      val inserted3 = customer.insert(record3)

      inserted1 should equal (Success(record1))
      inserted2 should equal (Success(record2))
      inserted3 should equal (Success(record3))
    }

    "allow for batch insert of records with and without missing values correctly" in {
      val inserted = customer.insertAll(Seq(record1, record2, record3))
      inserted should equal (Success(Seq(record1, record2, record3)))
    }

    "fail to insert records that do not adhere to the relations schema" in {
      customer.insert(record4).isFailure should equal (true)
    }

    "fail to batch insert records with at least one that does not adhere to the relations schema" in {
      customer.insertAll(Seq(record1, record2, record3, record4)).isFailure should equal (true)
    }

    "allow updating records with simple where" in {
      object Test extends RelationDef {
        val col1: ColumnDef[Int] = ColumnDef[Int]("ID")
        val col2: ColumnDef[String] = ColumnDef[String]("Firstname")
        val col3: ColumnDef[String] = ColumnDef[String]("Lastname")

        override val columns: Set[UntypedColumnDef] = Set(col1, col2, col3)
        override val name: String = "test"
      }
      val test = RowRelation(Test)

      import Test._
      test.insertAll(Seq(
        test.newRecord(col1 ~> 0 & col2 ~> "Firstname0" & col3 ~> "Lastname0").build(),
        test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        test.newRecord(col1 ~> 2 & col2 ~> "Firstname2" & col3 ~> "Lastname2").build(),
        test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        test.newRecord(col1 ~> 4 & col2 ~> "Firstname4" & col3 ~> "Lastname4").build()
      ))

      val result = test.update(col2 ~> "Hans" & col3 ~> "Maier")
        .where[Int](col1 -> { _ % 2 == 0 })
      result shouldBe Success(3)

      test.records shouldBe Success(Seq(
        test.newRecord(col1 ~> 0 & col2 ~> "Hans" & col3 ~> "Maier").build(),
        test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        test.newRecord(col1 ~> 2 & col2 ~> "Hans" & col3 ~> "Maier").build(),
        test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        test.newRecord(col1 ~> 4 & col2 ~> "Hans" & col3 ~> "Maier").build()
      ))
    }

    "allow updating records with multiple where conditions" in {
      object Test extends RelationDef {
        val col1: ColumnDef[Int] = ColumnDef[Int]("ID")
        val col2: ColumnDef[String] = ColumnDef[String]("Firstname")
        val col3: ColumnDef[String] = ColumnDef[String]("Lastname")

        override val columns: Set[UntypedColumnDef] = Set(col1, col2, col3)
        override val name: String = "test"
      }
      val test = RowRelation(Test)

      import Test._
      test.insertAll(Seq(
        test.newRecord(col1 ~> 0 & col2 ~> "Firstname0" & col3 ~> "Lastname0").build(),
        test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        test.newRecord(col1 ~> 2 & col2 ~> "Firstname2" & col3 ~> "Lastname2").build(),
        test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        test.newRecord(col1 ~> 4 & col2 ~> "Firstname4" & col3 ~> "Lastname4").build()
      ))

      val result = test.update(col2 ~> "Hans" & col3 ~> "Maier")
        .whereAll(
          Map(col1 -> { id: Any => id.asInstanceOf[Int] % 2 == 0 }) ++
          Map(col2 -> { firstname: Any => firstname.asInstanceOf[String] == "Firstname2" })
        )
      result shouldBe Success(1)

      test.records shouldBe Success(Seq(
        test.newRecord(col1 ~> 0 & col2 ~> "Firstname0" & col3 ~> "Lastname0").build(),
        test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        test.newRecord(col1 ~> 2 & col2 ~> "Hans" & col3 ~> "Maier").build(),
        test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        test.newRecord(col1 ~> 4 & col2 ~> "Firstname4" & col3 ~> "Lastname4").build()
      ))
    }

    "allow deletion of records" in {
      object Test extends RelationDef {
        val col1: ColumnDef[Int] = ColumnDef[Int]("ID")
        val col2: ColumnDef[String] = ColumnDef[String]("Firstname")
        val col3: ColumnDef[String] = ColumnDef[String]("Lastname")

        override val columns: Set[UntypedColumnDef] = Set(col1, col2, col3)
        override val name: String = "test"
      }
      val test = RowRelation(Test)

      import Test._
      test.insertAll(Seq(
        test.newRecord(col1 ~> 0 & col2 ~> "Firstname0" & col3 ~> "Lastname0").build(),
        test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        test.newRecord(col1 ~> 2 & col2 ~> "Firstname2" & col3 ~> "Lastname2").build(),
        test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        test.newRecord(col1 ~> 4 & col2 ~> "Firstname4" & col3 ~> "Lastname4").build()
      ))

      val record2Delete = test.newRecord(col1 ~> 2 & col2 ~> "Firstname2" & col3 ~> "Lastname2").build()
      val result = test.delete(record2Delete)
      result shouldBe Success(record2Delete)

      test.records shouldBe Success(Seq(
        test.newRecord(col1 ~> 0 & col2 ~> "Firstname0" & col3 ~> "Lastname0").build(),
        test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        test.newRecord(col1 ~> 4 & col2 ~> "Firstname4" & col3 ~> "Lastname4").build()
      ))
    }

    "throw error when trying to delete non-existing record" in {
      object Test extends RelationDef {
        val col1: ColumnDef[Int] = ColumnDef[Int]("ID")
        val col2: ColumnDef[String] = ColumnDef[String]("Firstname")
        val col3: ColumnDef[String] = ColumnDef[String]("Lastname")

        override val columns: Set[UntypedColumnDef] = Set(col1, col2, col3)
        override val name: String = "test"
      }
      val test = RowRelation(Test)

      import Test._
      test.insertAll(Seq(
        test.newRecord(col1 ~> 0 & col2 ~> "Firstname0" & col3 ~> "Lastname0").build(),
        test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        test.newRecord(col1 ~> 2 & col2 ~> "Firstname2" & col3 ~> "Lastname2").build(),
        test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        test.newRecord(col1 ~> 4 & col2 ~> "Firstname4" & col3 ~> "Lastname4").build()
      ))

      val record2Delete = test.newRecord(col1 ~> -1 & col2 ~> "" & col3 ~> "").build()
      val result = test.delete(record2Delete)
      result.isFailure shouldBe true
      result.failed.get match {
        case RecordNotFoundException(e) => e.contains("this relation does not contain the record") shouldBe true
        case t => fail(s"the wrong exception was thrown\nexpected: RecordNotFoundExcpetion\nfound: $t")
      }

      test.records shouldBe Success(Seq(
        test.newRecord(col1 ~> 0 & col2 ~> "Firstname0" & col3 ~> "Lastname0").build(),
        test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        test.newRecord(col1 ~> 2 & col2 ~> "Firstname2" & col3 ~> "Lastname2").build(),
        test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        test.newRecord(col1 ~> 4 & col2 ~> "Firstname4" & col3 ~> "Lastname4").build()
      ))
    }
  }

  // queries on and joins with data from RowRelation are performed in TransientRelation and therefore tested in its test
  // suite

}
