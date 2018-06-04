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

    "empty" should {
      val emptyRelation = TransientRelation(Seq.empty)

      "return an empty result set for any where or whereAll query" in {
        emptyRelation
          .where(colFirstname, (_: String) => true)
          .records shouldEqual Success(Seq.empty)

        emptyRelation.whereAll(Map(
          colFirstname.untyped -> {_: Any => true},
          colAge.untyped -> {_: Any => true}
        )).records shouldEqual Success(Seq.empty)
      }
    }

    "full" should {
      val fullRelation = TransientRelation(Seq(record1, record2))

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
          .isFailure should equal (true)

        fullRelation
          .project(columns + ColumnDef[Int]("bad-col"))
          .records
          .isFailure should equal (true)
      }
    }

    "joining with other TransientRelations" should {
      // FIXME the SSN column is never needed until now
      val colSSN: ColumnDef[String] = ColumnDef("SSN")
      val otherColumns: Set[UntypedColumnDef] = Set(colFirstname, colLastname, colSSN)

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

      val orders: Relation = TransientRelation(Seq(orderRecord1, orderRecord2, orderRecord3))
      val customers: Relation = TransientRelation(Seq(customerRecord1, customerRecord2))

      "return the appropriate result set for a leftJoin" in {
        orders
          .leftJoin(customers, (left, right) => left.get(colCustomerId) == right.get(colCustomerId))
          .records shouldEqual
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

      "return the appropriate result set for a rightJoin" in {
        orders
          .rightJoin(customers, (left, right) => left.get(colCustomerId) == right.get(colCustomerId))
          .records shouldEqual
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

      "return the appropriate result set for an innerJoin" in {
        orders
          .innerJoin(customers, (left, right) => left.get(colCustomerId) == right.get(colCustomerId))
          .records shouldEqual
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

      "return the appropriate result set for an outerJoin" in {
        orders
          .outerJoin(customers, (left, right) => left.get(colCustomerId) == right.get(colCustomerId))
          .records shouldEqual
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
    }

    "filled with incomplete records, i.e. missing values" should {
      val incompleteRelation = TransientRelation(Seq(record1, record2, record3, record4))

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
    }
  }
}
