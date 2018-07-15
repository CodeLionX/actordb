package de.up.hpi.informationsystems.adbms.relation

import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef, UntypedColumnDef}
import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.record.Record
import org.scalactic.Equality
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Success, Try}

class RelationBinOpsTest extends WordSpec with Matchers {

  // result sets are equal if their order is not!
  implicit val sortedRecordSeqEquality: Equality[Try[Seq[Record]]] = new Equality[Try[Seq[Record]]] {

    implicit val recordOrdering: Ordering[Record] = (x: Record, y: Record) => x.hashCode() - y.hashCode()

    def areEqual(a: Try[Seq[Record]], b: Any): Boolean =
      b match {
        case Success(r: Seq[Record]) => a.get.sorted.equals(r.sorted)
        case _ => false
      }
  }

  val colFirstname: ColumnDef[String] = ColumnDef("Firstname")
  val colLastname: ColumnDef[String] = ColumnDef("Lastname")
  val colId: ColumnDef[Int] = ColumnDef("ID")

  val columnSet1: Set[UntypedColumnDef] = Set(colId, colFirstname)
  val columnSet2: Set[UntypedColumnDef] = Set(colId, colLastname)

  val recSeq1: Seq[Record] = Seq(
    Record(columnSet1)(
      colId ~> 1 &
      colFirstname ~> "Hans"
    ).build(),
    Record(columnSet1)(
      colId ~> 2 &
      colFirstname ~> "Peter"
    ).build(),
    Record(columnSet1)(
      colId ~> 3 &
      colFirstname ~> "Lisa"
    ).build()
  )
  val recSeq2: Seq[Record] = Seq(
    Record(columnSet2)(
      colId ~> 1 &
      colLastname ~> "Maier"
    ).build(),
    Record(columnSet2)(
      colId ~> 2 &
      colLastname ~> "Schubert"
    ).build(),
    Record(columnSet2)(
      colId ~> 4 &
      colLastname ~> "Gross"
    ).build()
  )

  "A binary relation operation" when {

    // transient relations
    val TRunionRel = Relation(recSeq1)
    val TRjoinRel = Relation(recSeq2)

    // future relations
    val FRunionRel = FutureRelation.fromRecordSeq(Future{
      Thread.sleep(50)
      recSeq1
    })
    val FRjoinRel = FutureRelation.fromRecordSeq(Future{
      Thread.sleep(50)
      recSeq2
    })

    // mutable relations
    val RRunionRel = RowRelation(new RelationDef {
      override val name: String = "unionRel"
      override val columns: Set[UntypedColumnDef] = columnSet1
    })
    RRunionRel.insertAll(recSeq1)
    val RRjoinRel = RowRelation(new RelationDef {
      override val name: String = "joinRel"
      override val columns: Set[UntypedColumnDef] = columnSet2
    })
    RRjoinRel.insertAll(recSeq2)


    { // future relation tests
      val rel1 = FutureRelation.fromRecordSeq(Future {
        Thread.sleep(50)
        recSeq1
      })

      "operands: FutureRelation, FutureRelation" should {
        testUnionOperations(rel1, FRunionRel)
        testJoinOperations(rel1, FRjoinRel)
      }
      "operands: FutureRelation, TransientRelation" should {
        testUnionOperations(rel1, TRunionRel)
        testJoinOperations(rel1, TRjoinRel)
      }
      "operands: FutureRelation, MutableRelation" should {
        testUnionOperations(rel1, RRunionRel)
        testJoinOperations(rel1, RRjoinRel)
      }
    }

    { // transient relation tests
      val rel1 = Relation(recSeq1)

      "operands: TransientRelation, TransientRelation" should {
        testUnionOperations(rel1, TRunionRel)
        testJoinOperations(rel1, TRjoinRel)
      }
      "operands: TransientRelation, FutureRelation" should {
        testUnionOperations(rel1, FRunionRel)
        testJoinOperations(rel1, FRjoinRel)
      }
      "operands: TransientRelation, MutableRelation" should {
        testUnionOperations(rel1, RRunionRel)
        testJoinOperations(rel1, RRjoinRel)
      }
    }

    { // mutable relation tests
      val rel1 = RowRelation(new RelationDef {
        override val name: String = "rel1"
        override val columns: Set[UntypedColumnDef] = columnSet1
      })
      rel1.insertAll(recSeq1)

      "operands: MutableRelation, TransientRelation" should {
        testUnionOperations(rel1, TRunionRel)
        testJoinOperations(rel1, TRjoinRel)
      }
      "operands: MutableRelation, FutureRelation" should {
        testUnionOperations(rel1, FRunionRel)
        testJoinOperations(rel1, FRjoinRel)
      }
      "operands: MutableRelation, MutableRelation" should {
        testUnionOperations(rel1, RRunionRel)
        testJoinOperations(rel1, RRjoinRel)
      }
    }
  }

  def testUnionOperations(rel1: Relation, unionRel: Relation): Unit = {
    "support union" in {
      val res1 = rel1.union(unionRel)
      res1.columns shouldEqual columnSet1
      res1.records shouldEqual Success(recSeq1)
    }

    "support unionAll" in {
      val res2 = rel1.unionAll(unionRel)
      res2.columns shouldEqual columnSet1
      res2.records shouldEqual Success(recSeq1 ++ recSeq1)
    }
  }
  
  def testJoinOperations(rel1: Relation, joinRel: Relation): Unit = {
    val joinRecordBuilder = Record(columnSet1 ++ columnSet2)
    val joinIdCommonResult = Seq(
      joinRecordBuilder(
        colId ~> 1 &
          colFirstname ~> "Hans" &
          colLastname ~> "Maier"
      ).build(),
      joinRecordBuilder(
        colId ~> 2 &
          colFirstname ~> "Peter" &
          colLastname ~> "Schubert"
      ).build()
    )

    // inner joins
    "support inner join" in {
      val res3 = rel1.innerJoin(joinRel, (rec1, rec2) => rec1.get(colId) == rec2.get(colId))
      res3.columns shouldEqual columnSet1 ++ columnSet2
      res3.records shouldEqual Success(joinIdCommonResult)
    }

    "support inner equi join" in {
      val res4 = rel1.innerEquiJoin(joinRel, (colId, colId))
      res4.columns shouldEqual columnSet1 ++ columnSet2
      res4.records shouldEqual Success(joinIdCommonResult)
    }

    // outer joins
    "support full outer join" in {
      val res5 = rel1.outerJoin(joinRel, (rec1, rec2) => rec1.get(colId) == rec2.get(colId))
      res5.columns shouldEqual columnSet1 ++ columnSet2
      res5.records shouldEqual Success(joinIdCommonResult ++ Seq(
        joinRecordBuilder(
          colId ~> 3 &
            colFirstname ~> "Lisa"
        ).build(),
        joinRecordBuilder(
          colId ~> 4 &
            colLastname ~> "Gross"
        ).build()
      ))
    }

    "support left outer join" in {
      val res6 = rel1.leftJoin(joinRel, (rec1, rec2) => rec1.get(colId) == rec2.get(colId))
      res6.columns shouldEqual columnSet1 ++ columnSet2
      res6.records shouldEqual Success(joinIdCommonResult ++ Seq(
        joinRecordBuilder(
          colId ~> 3 &
            colFirstname ~> "Lisa"
        ).build()
      ))
    }

    "support right outer join" in {
      val res7 = rel1.rightJoin(joinRel, (rec1, rec2) => rec1.get(colId) == rec2.get(colId))
      res7.columns shouldEqual columnSet1 ++ columnSet2
      res7.records shouldEqual Success(joinIdCommonResult ++ Seq(
        joinRecordBuilder(
          colId ~> 4 &
            colLastname ~> "Gross"
        ).build()
      ))
    }
  }
}
