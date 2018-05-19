package de.up.hpi.informationsystems.adbms.definition

import org.scalatest.{Matchers, WordSpec}

import scala.util.{Failure, Success}

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

    "allow updating records with simple where" in {
      object Test extends RowRelation {
        val col1: ColumnDef[Int] = ColumnDef("ID")
        val col2: ColumnDef[String] = ColumnDef("Firstname")
        val col3: ColumnDef[String] = ColumnDef("Lastname")

        override val columns: Set[UntypedColumnDef] = Set(col1, col2, col3)
      }

      import Test._
      import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
      Test.insertAll(Seq(
        Test.newRecord(col1 ~> 0 & col2 ~> "Firstname0" & col3 ~> "Lastname0").build(),
        Test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        Test.newRecord(col1 ~> 2 & col2 ~> "Firstname2" & col3 ~> "Lastname2").build(),
        Test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        Test.newRecord(col1 ~> 4 & col2 ~> "Firstname4" & col3 ~> "Lastname4").build()
      ))

      val result = Test.update(col2 ~> "Hans" & col3 ~> "Maier")
        .where[Int](col1 -> { _ % 2 == 0 })
      result shouldBe Success(3)

      Test.records shouldBe Success(Seq(
        Test.newRecord(col1 ~> 0 & col2 ~> "Hans" & col3 ~> "Maier").build(),
        Test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        Test.newRecord(col1 ~> 2 & col2 ~> "Hans" & col3 ~> "Maier").build(),
        Test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        Test.newRecord(col1 ~> 4 & col2 ~> "Hans" & col3 ~> "Maier").build()
      ))
    }

    "allow updating records with multiple where conditions" in {
      object Test extends RowRelation {
        val col1: ColumnDef[Int] = ColumnDef("ID")
        val col2: ColumnDef[String] = ColumnDef("Firstname")
        val col3: ColumnDef[String] = ColumnDef("Lastname")

        override val columns: Set[UntypedColumnDef] = Set(col1, col2, col3)
      }

      import Test._
      import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
      Test.insertAll(Seq(
        Test.newRecord(col1 ~> 0 & col2 ~> "Firstname0" & col3 ~> "Lastname0").build(),
        Test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        Test.newRecord(col1 ~> 2 & col2 ~> "Firstname2" & col3 ~> "Lastname2").build(),
        Test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        Test.newRecord(col1 ~> 4 & col2 ~> "Firstname4" & col3 ~> "Lastname4").build()
      ))

      val result = Test.update(col2 ~> "Hans" & col3 ~> "Maier")
        .whereAll(
          Map(col1.untyped -> { id: Any => id.asInstanceOf[Int] % 2 == 0 }) ++
          Map(col2.untyped -> { firstname: Any => firstname.asInstanceOf[String] == "Firstname2" })
        )
      result shouldBe Success(1)

      Test.records shouldBe Success(Seq(
        Test.newRecord(col1 ~> 0 & col2 ~> "Firstname0" & col3 ~> "Lastname0").build(),
        Test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        Test.newRecord(col1 ~> 2 & col2 ~> "Hans" & col3 ~> "Maier").build(),
        Test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        Test.newRecord(col1 ~> 4 & col2 ~> "Firstname4" & col3 ~> "Lastname4").build()
      ))
    }

    "allow deletion of records" in {
      object Test extends RowRelation {
        val col1: ColumnDef[Int] = ColumnDef("ID")
        val col2: ColumnDef[String] = ColumnDef("Firstname")
        val col3: ColumnDef[String] = ColumnDef("Lastname")

        override val columns: Set[UntypedColumnDef] = Set(col1, col2, col3)
      }

      import Test._
      import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
      Test.insertAll(Seq(
        Test.newRecord(col1 ~> 0 & col2 ~> "Firstname0" & col3 ~> "Lastname0").build(),
        Test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        Test.newRecord(col1 ~> 2 & col2 ~> "Firstname2" & col3 ~> "Lastname2").build(),
        Test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        Test.newRecord(col1 ~> 4 & col2 ~> "Firstname4" & col3 ~> "Lastname4").build()
      ))

      val record2Delete = Test.newRecord(col1 ~> 2 & col2 ~> "Firstname2" & col3 ~> "Lastname2").build()
      val result = Test.delete(record2Delete)
      result shouldBe Success(record2Delete)

      Test.records shouldBe Success(Seq(
        Test.newRecord(col1 ~> 0 & col2 ~> "Firstname0" & col3 ~> "Lastname0").build(),
        Test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        Test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        Test.newRecord(col1 ~> 4 & col2 ~> "Firstname4" & col3 ~> "Lastname4").build()
      ))
    }

    "throw error when trying to delete non-existing record" in {
      object Test extends RowRelation {
        val col1: ColumnDef[Int] = ColumnDef("ID")
        val col2: ColumnDef[String] = ColumnDef("Firstname")
        val col3: ColumnDef[String] = ColumnDef("Lastname")

        override val columns: Set[UntypedColumnDef] = Set(col1, col2, col3)
      }

      import Test._
      import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
      Test.insertAll(Seq(
        Test.newRecord(col1 ~> 0 & col2 ~> "Firstname0" & col3 ~> "Lastname0").build(),
        Test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        Test.newRecord(col1 ~> 2 & col2 ~> "Firstname2" & col3 ~> "Lastname2").build(),
        Test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        Test.newRecord(col1 ~> 4 & col2 ~> "Firstname4" & col3 ~> "Lastname4").build()
      ))

      val record2Delete = Test.newRecord(col1 ~> -1 & col2 ~> "" & col3 ~> "").build()
      val result = Test.delete(record2Delete)
      result.isFailure shouldBe true
      result.failed.get match {
        case RecordNotFoundException(e) => e.contains("this relation does not contain the record") shouldBe true
        case t => fail(s"the wrong exception was thrown\nexpected: RecordNotFoundExcpetion\nfound: $t")
      }

      Test.records shouldBe Success(Seq(
        Test.newRecord(col1 ~> 0 & col2 ~> "Firstname0" & col3 ~> "Lastname0").build(),
        Test.newRecord(col1 ~> 1 & col2 ~> "Firstname1" & col3 ~> "Lastname1").build(),
        Test.newRecord(col1 ~> 2 & col2 ~> "Firstname2" & col3 ~> "Lastname2").build(),
        Test.newRecord(col1 ~> 3 & col2 ~> "Firstname3" & col3 ~> "Lastname3").build(),
        Test.newRecord(col1 ~> 4 & col2 ~> "Firstname4" & col3 ~> "Lastname4").build()
      ))
    }
  }

  // queries on data from RowRelation are performed in TransientRelation and therefore tested in its test suite

}
