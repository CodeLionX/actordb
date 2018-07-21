package de.up.hpi.informationsystems.adbms.relation

import java.lang.StringIndexOutOfBoundsException

import de.up.hpi.informationsystems.adbms.RecordNotFoundException
import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef}
import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.record.Record
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class RowRelationTest extends WordSpec with Matchers {

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

  "A row relation considered as mutable relation" should {

    object Test extends RelationDef {
      val col1: ColumnDef[Int] = ColumnDef[Int]("ID")
      val col2: ColumnDef[String] = ColumnDef[String]("Firstname")
      val col3: ColumnDef[String] = ColumnDef[String]("Lastname")

      override val columns: Set[UntypedColumnDef] = Set(col1, col2, col3)
      override val name: String = "test"
    }

    val testRecords = Seq(
      Test.newRecord(Test.col1 ~> 0 & Test.col2 ~> "Firstname0" & Test.col3 ~> "Lastname0").build(),
      Test.newRecord(Test.col1 ~> 1 & Test.col2 ~> "Firstname1" & Test.col3 ~> "Lastname1").build(),
      Test.newRecord(Test.col1 ~> 2 & Test.col2 ~> "Firstname2" & Test.col3 ~> "Lastname2").build(),
      Test.newRecord(Test.col1 ~> 3 & Test.col2 ~> "Firstname3" & Test.col3 ~> "Lastname3").build(),
      Test.newRecord(Test.col1 ~> 4 & Test.col2 ~> "Firstname4" & Test.col3 ~> "Lastname4").build()
    )

    "insert records with and without missing values correctly" in {
      val inserted1 = customer.insert(record1)
      val inserted2 = customer.insert(record2)
      val inserted3 = customer.insert(record3)

      inserted1 should equal(Success(record1))
      inserted2 should equal(Success(record2))
      inserted3 should equal(Success(record3))
    }

    "allow for batch insert of records with and without missing values correctly" in {
      val inserted = customer.insertAll(Seq(record1, record2, record3))
      inserted should equal(Success(Seq(record1, record2, record3)))
    }

    "fail to insert records that do not adhere to the relations schema" in {
      val test = RowRelation(Customer)
      test.insert(record4).isFailure should equal (true)
      test.records shouldEqual Success(Seq.empty)
    }

    "fail to batch insert records with at least one that does not adhere to the relations schema" in {
      customer.insertAll(Seq(record1, record2, record3, record4)).isFailure should equal(true)
    }

    "allow updating records with simple where" in {
      val test = RowRelation(Test)
      test.insertAll(testRecords)

      val result = test.update(Test.col2 ~> "Hans" & Test.col3 ~> "Maier")
        .where[Int](Test.col1 -> {
        _ % 2 == 0
      })

      result shouldBe Success(3)
      test.records shouldBe Success(Seq(
        test.newRecord(Test.col1 ~> 0 & Test.col2 ~> "Hans" & Test.col3 ~> "Maier").build(),
        test.newRecord(Test.col1 ~> 1 & Test.col2 ~> "Firstname1" & Test.col3 ~> "Lastname1").build(),
        test.newRecord(Test.col1 ~> 2 & Test.col2 ~> "Hans" & Test.col3 ~> "Maier").build(),
        test.newRecord(Test.col1 ~> 3 & Test.col2 ~> "Firstname3" & Test.col3 ~> "Lastname3").build(),
        test.newRecord(Test.col1 ~> 4 & Test.col2 ~> "Hans" & Test.col3 ~> "Maier").build()
      ))
    }

    "allow updating records with multiple where conditions" in {
      val test = RowRelation(Test)
      test.insertAll(testRecords)

      val result = test.update(Test.col2 ~> "Hans" & Test.col3 ~> "Maier")
        .whereAll(
          Map(Test.col1 -> { id: Any => id.asInstanceOf[Int] % 2 == 0 }) ++
            Map(Test.col2 -> { firstname: Any => firstname.asInstanceOf[String] == "Firstname2" })
        )

      result shouldBe Success(1)
      test.records shouldBe Success(Seq(
        test.newRecord(Test.col1 ~> 0 & Test.col2 ~> "Firstname0" & Test.col3 ~> "Lastname0").build(),
        test.newRecord(Test.col1 ~> 1 & Test.col2 ~> "Firstname1" & Test.col3 ~> "Lastname1").build(),
        test.newRecord(Test.col1 ~> 2 & Test.col2 ~> "Hans" & Test.col3 ~> "Maier").build(),
        test.newRecord(Test.col1 ~> 3 & Test.col2 ~> "Firstname3" & Test.col3 ~> "Lastname3").build(),
        test.newRecord(Test.col1 ~> 4 & Test.col2 ~> "Firstname4" & Test.col3 ~> "Lastname4").build()
      ))
    }

    "allow deletion of records" in {
      val test = RowRelation(Test)
      test.insertAll(testRecords)

      val record2Delete = test.newRecord(Test.col1 ~> 2 & Test.col2 ~> "Firstname2" & Test.col3 ~> "Lastname2").build()
      val result = test.delete(record2Delete)
      result shouldBe Success(record2Delete)

      test.records shouldBe Success(testRecords.filterNot(_ == record2Delete))
    }

    "throw error when trying to delete non-existing record" in {
      val test = RowRelation(Test)
      test.insertAll(testRecords)

      val record2Delete = test.newRecord(Test.col1 ~> -1 & Test.col2 ~> "" & Test.col3 ~> "").build()
      val result = test.delete(record2Delete)
      result.isFailure shouldBe true
      result.failed.get match {
        case RecordNotFoundException(e) => e.contains("this relation does not contain the record") shouldBe true
        case t => fail(s"the wrong exception was thrown\nexpected: RecordNotFoundExcpetion\nfound: $t")
      }

      test.records shouldBe Success(testRecords)
    }
  }

  "A row relation considered as Relation" when {

    "empty" should {
      val emptyRelation = RowRelation(Customer)

      "return an empty result set for any project call" in {
        emptyRelation
          .project(Set(Customer.colLastname))
          .records shouldEqual Success(Seq.empty)
      }

      "return an empty result set for any where query" in {
        emptyRelation
          .where(Customer.colFirstname, (_: String) => true)
          .records shouldEqual Success(Seq.empty)
      }

      "return an empty result set for any whereAll query" in {
        emptyRelation.whereAll(Map(
          Customer.colFirstname -> { _ => true },
          Customer.colAge -> { _ => true }
        )).records shouldEqual Success(Seq.empty)
      }

      "return an empty result set for any applyOn function" in {
        emptyRelation
          .applyOn(Customer.colFirstname, (_: String) => "something bad!")
          .records shouldEqual Success(Seq.empty)
      }
    }

    "full" should {
      val fullRelation = RowRelation(Customer)
      fullRelation.insertAll(Seq(record1, record2))

      "return an appropriate result set for a project call" in {
        val lastnameBuilder = Record(Set(Customer.colLastname))
        val result = fullRelation.project(Set(Customer.colLastname))

        result.records shouldEqual Success(Seq(
          lastnameBuilder(Customer.colLastname ~> "Test").build(),
          lastnameBuilder(Customer.colLastname ~> "Mustermann").build()
        ))
      }

      "return an appropriate result set for a where query" in {
        val result = fullRelation.where(Customer.colFirstname, (name: String) => name == "Max" )

        result.records shouldEqual Success(Seq(record2))
      }

      "return an appropriate result set for a whereAll query" in {
        val result = fullRelation.whereAll(Map(
          Customer.colFirstname ->  { case name: String => name == "Max" },
          Customer.colAge -> { case age: Int => age > 20 }
        ))

        result.records shouldEqual Success(Seq(record2))
      }

      "return an appropriate result set for an applyOn function" in {
        val result = fullRelation.applyOn(Customer.colFirstname, (name: String) => name.substring(0, 2))

        result.records shouldEqual Success(Seq(
          Customer.newRecord(
            Customer.colFirstname ~> "Te" & Customer.colLastname ~> "Test" & Customer.colAge ~> 42
          ).build(),
          Customer.newRecord(
            Customer.colFirstname ~> "Ma" & Customer.colLastname ~> "Mustermann" & Customer.colAge ~> 23
          ).build()
        ))
      }
    }

    "filled with incomplete records" should {
      val relation = RowRelation(Customer)
      relation.insertAll(Seq(record1, record2, record3))

      "return an appropriate result set for a project call" in {
        val firstnameBuilder = Record(Set(Customer.colFirstname))
        val result = relation.project(Set(Customer.colFirstname))

        result.records shouldEqual Success(Seq(
          firstnameBuilder(Customer.colFirstname ~> "Test").build(),
          firstnameBuilder(Customer.colFirstname ~> "Max").build(),
          firstnameBuilder.build()
        ))
      }

      "return an appropriate result set for a where query" in {
        val result = relation.where(Customer.colFirstname, (name: String) => name == "Max" )

        result.records shouldEqual Success(Seq(record2))
      }

      "return an appropriate result set for a whereAll query" in {
        val result = relation.whereAll(Map(
          Customer.colFirstname ->  { case name: String => name == "Max" },
          Customer.colAge -> { case age: Int => age > 20 }
        ))

        result.records shouldEqual Success(Seq(record2))
      }

      "return an failure for a bad applyOn function" in {
        val result = relation
          .applyOn(Customer.colFirstname, (name: String) => name.substring(0, 2))
          .records

        result.isFailure shouldBe true
        result.failed.get match {
          case e: StringIndexOutOfBoundsException => e.getMessage.contains("String index out of range") shouldBe true
          case t => fail(s"the wrong exception was thrown\nexpected: StringIndexOutOfBoundsException\nfound: $t")
        }
      }

      "return an appropriate result set for a good applyOn function" in {
        val result = relation.applyOn(Customer.colFirstname, {
          case name: String if name.length < 2 => Customer.colFirstname.default
          case name: String => name.substring(0, 2)
        }: String => String)

        result.records shouldEqual Success(Seq(
          Customer.newRecord(
            Customer.colFirstname ~> "Te" & Customer.colLastname ~> "Test" & Customer.colAge ~> 42
          ).build(),
          Customer.newRecord(
            Customer.colFirstname ~> "Ma" & Customer.colLastname ~> "Mustermann" & Customer.colAge ~> 23
          ).build(),
          Customer.newRecord(
            Customer.colFirstname ~> "" & Customer.colLastname ~> "Doe" & Customer.colAge ~> 200215
          ).build()
        ))
      }
    }
    "fail on where query with wrong column definition" in {
      val customer = RowRelation(Customer)
      customer.insert(record1)

      val result = customer.where(ColumnDef[Double]("WRONG") -> { d: Double => d == 2.1}).records

      result.isFailure shouldBe true
    }

    "fail on whereAll query with wrong column definition" in {
      val customer = RowRelation(Customer)
      customer.insert(record1)

      val result = customer.whereAll(
        Map(ColumnDef[Double]("WRONG") -> ({ case d: Double => d == 2.1}: Any => Boolean))
      ).records

      result.isFailure shouldBe true
    }

    "fail on project query with wrong column definition" in {
      val customer = RowRelation(Customer)
      customer.insert(record1)

      val result = customer.project(
        Set(ColumnDef[Double]("WRONG"))
      ).records

      result.isFailure shouldBe true
    }
  }

  // joins with data from RowRelation are performed in RelationBinOpsTest and therefore tested in its test suite

}
