package de.up.hpi.informationsystems.adbms.definition

import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class TransientRelationTest extends WordSpec with Matchers {

  "A transient relation" when {

    val colFirstname: ColumnDef[String] = ColumnDef("Firstname")
    val colLastname: ColumnDef[String] = ColumnDef("Lastname")
    val colAge: ColumnDef[Int] = ColumnDef("Age")
    
    val columns: Set[UntypedColumnDef] = Set(colFirstname, colLastname, colAge)

    val record1 = Record(columns)
      .withCellContent(colFirstname)("Test")
      .withCellContent(colLastname)("Test")
      .withCellContent(colAge)(42)
      .build()

    val record2 = Record(Set(colFirstname, colLastname, colAge))
      .withCellContent(colFirstname)("Max")
      .withCellContent(colLastname)("Mustermann")
      .withCellContent(colAge)(23)
      .build()

    val record3 = Record(columns)
      // missing firstName
      .withCellContent(colLastname)("es")
      .withCellContent(colAge)(200215)
      .build()

    val record4 = Record(columns)
      .withCellContent(colFirstname)(null)
      .withCellContent(colAge)(2)
      .build()

    val colOrderId: ColumnDef[Int] = ColumnDef("OrderId")
    val colOrderdate: ColumnDef[String] = ColumnDef("Orderdate")
    val colCustomerId: ColumnDef[Int] = ColumnDef("CustomerId")
    val colFullname: ColumnDef[String] = ColumnDef("Fullname")
    val colCountry: ColumnDef[String] = ColumnDef("Country")

    val orderColumns: Set[UntypedColumnDef] = Set(colOrderId, colOrderdate, colCustomerId)
    val customerColumns: Set[UntypedColumnDef] = Set(colCustomerId, colFullname, colCountry)

    val orderRecord1 = Record(orderColumns)
      .withCellContent(colOrderId)(504)
      .withCellContent(colOrderdate)("05/06/07")
      .withCellContent(colCustomerId)(14)
      .build()

    val orderRecord2 = Record(orderColumns)
      .withCellContent(colOrderId)(505)
      .withCellContent(colOrderdate)("08/06/07")
      .withCellContent(colCustomerId)(14)
      .build()

    val orderRecord3 = Record(orderColumns)
      .withCellContent(colOrderId)(504)
      .withCellContent(colOrderdate)("17/06/07")
      .withCellContent(colCustomerId)(6)
      .build()

    val customerRecord1 = Record(customerColumns)
      .withCellContent(colCustomerId)(14)
      .withCellContent(colFullname)("Max Mustermann")
      .withCellContent(colCountry)("Germany")
      .build()

    val customerRecord2 = Record(customerColumns)
      .withCellContent(colCustomerId)(7)
      .withCellContent(colFullname)("Omari Wesson")
      .withCellContent(colCountry)("USA")
      .build()

    "empty" should {
      val emptyRelation = Relation(Seq.empty)

      "return an empty result set for any where query" in {
        emptyRelation
          .where(colFirstname, (_: String) => true)
          .records shouldEqual Success(Seq.empty)
      }

      "return an empty result set for any whereAll query" in {
        emptyRelation.whereAll(Map(
          colFirstname.untyped -> {_: Any => true},
          colAge.untyped -> {_: Any => true}
        )).records shouldEqual Success(Seq.empty)
      }

      "fail when joined with itself" in {
        emptyRelation
          .innerEquiJoin(emptyRelation, (colFirstname, colFirstname))
          .records
          .isFailure shouldBe true
      }

      "return an empty result set for any applyOn function" in {
        emptyRelation
          .applyOn(colFirstname, (name: String) => "something bad!")
          .records shouldEqual Success(Seq.empty)
      }
    }

    "full" should {
      val fullRelation = Relation(Seq(record1, record2))

      "return the appropriate result set for a where query including the empty result set" in {
        fullRelation
          .where(colFirstname, (_: String) => true)
          .records shouldEqual Success(Seq(record1, record2))

        fullRelation
          .where(colAge, (id: Int) => id == 23)
          .records shouldEqual Success(Seq(record2))

        fullRelation
          .where(colAge, (id: Int) => id > 42)
          .records shouldEqual Success(Seq.empty)
      }

      "return the appropriate result set for a whereAll query including the empty result set" in {
        fullRelation.whereAll(Map(
            colAge.untyped -> {id: Any => id.asInstanceOf[Int] <= 23},
            colFirstname.untyped -> {field: Any => field.asInstanceOf[String].contains("Max")}
        )).records shouldEqual Success(Seq(record2))
      }

      "return selected columns only from project" in {
        fullRelation.project(Set(colFirstname)).records shouldEqual
          Success(Seq(
            record1.project(Set(colFirstname)).get,
            record2.project(Set(colFirstname)).get
          ))
      }

      "fail to project to non-existent columns" in {
        fullRelation
          .project(Set(ColumnDef[Int]("bad-col")))
          .records
          .isFailure shouldBe true

        fullRelation
          .project(columns + ColumnDef[Int]("bad-col"))
          .records
          .isFailure shouldBe true
      }

      "return an appropriate result set for an applyOn function" in {
        fullRelation
          .applyOn(colFirstname, (name: String) => name + " test")
          .records shouldEqual Success(Seq(
            record1.updated(colFirstname, "Test test"),
            record2.updated(colFirstname, "Max test")
          ))
      }

      /* Joins */
      /* Cross-join */

      "fail when inner-equi-joined with an empty relation" in {
        fullRelation
          .innerEquiJoin(Relation(Seq.empty), (colFirstname, colFirstname))
          .records
          .isFailure shouldBe true
      }

      "return an appropriate result for inner-equi-join with itself with different columns" in {
        val diffColumnsJoined = fullRelation
          .innerEquiJoin(fullRelation, (colFirstname, colLastname))

        diffColumnsJoined.columns shouldEqual columns
        diffColumnsJoined.records shouldEqual Success(Seq(record1))
      }

      "return itself for join with itself on same column" in {
        val sameColumnJoined = fullRelation
          .innerEquiJoin(fullRelation, (colFirstname, colFirstname))

        sameColumnJoined.columns shouldEqual columns
        sameColumnJoined.records shouldEqual Success(Seq(record1, record2))
      }

      "return appropriate result for inner-equi-join" in {
        val colFirstname2 = ColumnDef[String]("Firstname2")
        val col1 = ColumnDef[Double]("col1")

        val otherRecord1 = Record(Set(colFirstname2, col1))
          .withCellContent(colFirstname2)("Test")
          .withCellContent(col1)(12.1)
          .build()

        val otherRecord2 = Record(Set(colFirstname2, col1))
          .withCellContent(colFirstname2)("Test")
          .withCellContent(col1)(916.93)
          .build()

        val otherRel = Relation(Seq(otherRecord1, otherRecord2))
        val sameColumnJoined = fullRelation
          .innerEquiJoin(otherRel, (colFirstname, colFirstname2))

        sameColumnJoined.columns shouldEqual columns + colFirstname2 + col1
        sameColumnJoined.records shouldEqual Success(Seq(
          record1 ++ otherRecord1,
          record1 ++ otherRecord2
        ))
      }

      "fail to inner-equi-join on wrong column definition" in {
        val joined1 = fullRelation
          .innerEquiJoin(fullRelation, (ColumnDef[String]("something"), colFirstname))
          .records
        val joined2 = fullRelation
          .innerEquiJoin(fullRelation, (colFirstname, ColumnDef[String]("something")))
          .records

        joined1.isFailure shouldBe true
        joined2.isFailure shouldBe true
      }

      /* innerJoin */

      "return an appropriate result for innerJoin with itself using different column values" in {
        val diffColumnsJoined = fullRelation
          .innerJoin(fullRelation, (lside, rside) => lside.get(colFirstname).get == rside.get(colLastname).get)

        diffColumnsJoined.columns shouldEqual columns
        diffColumnsJoined.records shouldEqual Success(Seq(record1))
      }

      "return itself for innerJoin with itself using the same column values" in {
        val sameColumnJoined = fullRelation
          .innerJoin(fullRelation, (left, right) => left.get(colFirstname).get == right.get(colFirstname).get)

        sameColumnJoined.columns shouldEqual columns
        sameColumnJoined.records shouldEqual Success(Seq(record1, record2))
      }

      "return the appropriate result for an innerJoin" in {
        val orders: Relation = Relation(Seq(orderRecord1, orderRecord2, orderRecord3))
        val customers: Relation = Relation(Seq(customerRecord1, customerRecord2))

        val joined = orders
          .innerJoin(customers, (left, right) => left.get(colCustomerId) == right.get(colCustomerId))

        joined.columns shouldEqual orders.columns ++ customers.columns
        joined.records shouldEqual
          Success(Seq(
            Record(orderColumns ++ customerColumns)
              .withCellContent(colCustomerId)(14)
              .withCellContent(colFullname)("Max Mustermann")
              .withCellContent(colCountry)("Germany")
              .withCellContent(colOrderId)(504)
              .withCellContent(colOrderdate)("05/06/07")
              .build(),
            Record(orderColumns ++ customerColumns)
              .withCellContent(colCustomerId)(14)
              .withCellContent(colFullname)("Max Mustermann")
              .withCellContent(colCountry)("Germany")
              .withCellContent(colOrderId)(505)
              .withCellContent(colOrderdate)("08/06/07")
              .build()
          ))
      }

      "fail to innerJoin on wrong column definition" in {
        val joined1 = fullRelation
          .innerJoin(fullRelation, (left, right) => left.get(ColumnDef[String]("something")).get == right.get(colFirstname).get)
          .records
        val joined2 = fullRelation
          .innerJoin(fullRelation, (left, right) => left.get(colFirstname).get == right.get(ColumnDef[String]("something")).get)
          .records

        joined1.isFailure shouldBe true
        joined2.isFailure shouldBe true
      }

      /* outerJoin */

      "return the appropriate result for an outerJoin" in {
        val orders: Relation = Relation(Seq(orderRecord1, orderRecord2, orderRecord3))
        val customers: Relation = Relation(Seq(customerRecord1, customerRecord2))

        val joined = orders
          .outerJoin(customers, (left, right) => left.get(colCustomerId) == right.get(colCustomerId))

        joined.columns shouldEqual orders.columns ++ customers.columns
        joined.records shouldEqual
          Success(Seq(
            Record(orderColumns ++ customerColumns)
              .withCellContent(colCustomerId)(14)
              .withCellContent(colFullname)("Max Mustermann")
              .withCellContent(colCountry)("Germany")
              .withCellContent(colOrderId)(504)
              .withCellContent(colOrderdate)("05/06/07")
              .build(),
            Record(orderColumns ++ customerColumns)
              .withCellContent(colCustomerId)(14)
              .withCellContent(colFullname)("Max Mustermann")
              .withCellContent(colCountry)("Germany")
              .withCellContent(colOrderId)(505)
              .withCellContent(colOrderdate)("08/06/07")
              .build(),
            Record(orderColumns ++ customerColumns)
              .withCellContent(colOrderId)(504)
              .withCellContent(colOrderdate)("17/06/07")
              .withCellContent(colCustomerId)(6)
              .build(),
            Record(orderColumns ++ customerColumns)
              .withCellContent(colCustomerId)(7)
              .withCellContent(colFullname)("Omari Wesson")
              .withCellContent(colCountry)("USA")
              .build()
          ))
      }

      "fail to outerJoin on wrong column definition" in {
        val joined1 = fullRelation
          .outerJoin(fullRelation, (left, right) => left.get(ColumnDef[String]("something")).get == right.get(colFirstname).get)
          .records
        val joined2 = fullRelation
          .outerJoin(fullRelation, (left, right) => left.get(colFirstname).get == right.get(ColumnDef[String]("something")).get)
          .records

        joined1.isFailure shouldBe true
        joined2.isFailure shouldBe true
      }

      /* leftJoin */

      "return the appropriate result set for a leftJoin" in {
        val orders: Relation = Relation(Seq(orderRecord1, orderRecord2, orderRecord3))
        val customers: Relation = Relation(Seq(customerRecord1, customerRecord2))

        val joined = orders
          .leftJoin(customers, (left, right) => left.get(colCustomerId) == right.get(colCustomerId))

        joined.columns shouldEqual orders.columns ++ customers.columns
        joined.records shouldEqual
          Success(Seq(
            Record(orderColumns ++ customerColumns)
              .withCellContent(colOrderId)(504)
              .withCellContent(colOrderdate)("05/06/07")
              .withCellContent(colCustomerId)(14)
              .withCellContent(colFullname)("Max Mustermann")
              .withCellContent(colCountry)("Germany")
              .build(),
            Record(orderColumns ++ customerColumns)
              .withCellContent(colOrderId)(505)
              .withCellContent(colOrderdate)("08/06/07")
              .withCellContent(colCustomerId)(14)
              .withCellContent(colFullname)("Max Mustermann")
              .withCellContent(colCountry)("Germany")
              .build(),
            Record(orderColumns ++ customerColumns)
              .withCellContent(colOrderId)(504)
              .withCellContent(colOrderdate)("17/06/07")
              .withCellContent(colCustomerId)(6)
              .build()
          ))
      }

      "fail to leftJoin on wrong column definition" in {
        val joined1 = fullRelation
          .leftJoin(fullRelation, (left, right) => left.get(ColumnDef[String]("something")).get == right.get(colFirstname).get)
          .records
        val joined2 = fullRelation
          .leftJoin(fullRelation, (left, right) => left.get(colFirstname).get == right.get(ColumnDef[String]("something")).get)
          .records

        joined1.isFailure shouldBe true
        joined2.isFailure shouldBe true
      }

      /* rightJoin */

      "return the appropriate result set for a rightJoin" in {
        val orders: Relation = Relation(Seq(orderRecord1, orderRecord2, orderRecord3))
        val customers: Relation = Relation(Seq(customerRecord1, customerRecord2))

        val joined = orders
          .rightJoin(customers, (left, right) => left.get(colCustomerId) == right.get(colCustomerId))

        joined.columns shouldEqual orders.columns ++ customers.columns
        joined.records shouldEqual
          Success(Seq(
            Record(orderColumns ++ customerColumns)
              .withCellContent(colCustomerId)(14)
              .withCellContent(colFullname)("Max Mustermann")
              .withCellContent(colCountry)("Germany")
              .withCellContent(colOrderId)(504)
              .withCellContent(colOrderdate)("05/06/07")
              .build(),
            Record(orderColumns ++ customerColumns)
              .withCellContent(colCustomerId)(14)
              .withCellContent(colFullname)("Max Mustermann")
              .withCellContent(colCountry)("Germany")
              .withCellContent(colOrderId)(505)
              .withCellContent(colOrderdate)("08/06/07")
              .build(),
            Record(orderColumns ++ customerColumns)
              .withCellContent(colCustomerId)(7)
              .withCellContent(colFullname)("Omari Wesson")
              .withCellContent(colCountry)("USA")
              .build()
          ))
      }

      "fail to rightJoin on wrong column definition" in {
        val joined1 = fullRelation
          .rightJoin(fullRelation, (left, right) => left.get(ColumnDef[String]("something")).get == right.get(colFirstname).get)
          .records
        val joined2 = fullRelation
          .rightJoin(fullRelation, (left, right) => left.get(colFirstname).get == right.get(ColumnDef[String]("something")).get)
          .records

        joined1.isFailure shouldBe true
        joined2.isFailure shouldBe true
      }
    }

    "filled with incomplete records, i.e. missing values" should {
      val incompleteRelation = Relation(Seq(record1, record2, record3, record4))

      "return the appropriate result set for a where query" when {

        "matching all" in {
          incompleteRelation
            .where(colAge, (_: Any) => true)
            .records shouldEqual Success(Seq(record1, record2, record3, record4))
        }

        "smaller result set" in {
          incompleteRelation
            .where(colAge, (id: Int) => id >= 42)
            .records shouldEqual Success(Seq(record1, record3))
        }

        "empty result set" in {
          incompleteRelation
            .where(colAge, (id: Int) => id < 2)
            .records shouldEqual Success(Seq.empty)
        }

        "condition on column with null-value" in {
          incompleteRelation
            .where(colFirstname, (field: String) => field.contains("Test"))
            .records shouldEqual Success(Seq(record1))
        }
      }

      "return the appropriate result set for a whereAll query including the empty result set" in {
        pending // NullPointerException
        incompleteRelation.whereAll(
          Map(
            colAge.untyped -> {age: Any => age.asInstanceOf[Int] > 40},
            colLastname.untyped -> {field: Any => field.asInstanceOf[String].contains("es")}
          )).records shouldEqual Success(Seq(record1, record3))
        }

      "return selected columns only from project including records with null-values" in {
        pending // NullPointerException
        // deduce correctness of Relation.project from correctness of Record.project
        incompleteRelation.project(Set(colFirstname)).records shouldEqual
          Success(Seq(
            record1.project(Set(colFirstname)).get,
            record2.project(Set(colFirstname)).get,
            record3.project(Set(colFirstname)).get
          ))
      }

      "return the appropriate result set for a whereAll query including condition on null-value column" in {
        pending // NullPointerException
        incompleteRelation.whereAll(
          Map(
            colAge.untyped -> {id: Any => id.asInstanceOf[Int] <= 2},
            colFirstname.untyped -> {field: Any => field.asInstanceOf[String].contains("esty")}
          )).records shouldEqual Success(Seq.empty)
      }

      "return an appropriate result set for an applyOn function" in {
        incompleteRelation
          .applyOn(colFirstname, (name: String) => name + " test")
          .records shouldEqual Success(Seq(
          record1.updated(colFirstname, "Test test"),
          record2.updated(colFirstname, "Max test"),
          record3,  // missing
          record4   // null
        ))
      }
    }
  }
}
