package de.up.hpi.informationsystems.adbms.definition

import akka.actor.ActorRef

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Awaitable, CanAwait, Future}
import scala.language.postfixOps
import scala.util.Try


trait FutureRelation extends Relation with Immutable with Awaitable[Try[Seq[Record]]] {

  override def innerJoin(other: Relation, on: Relation.RecordComparator): FutureRelation

  def pipeTo(actor: ActorRef): Unit

  def future: Future[Try[Seq[Record]]]

  def transform(f: Relation => Relation): FutureRelation

}

object FutureRelation {

  type BinRelationOp = (Relation, Relation) => Relation

  val defaultTimeout: Duration = 5 seconds

  def fromRecordSeq(data: Future[Seq[Record]]): FutureRelation =
    apply(data.map(d => TransientRelation(d)))
  def fromRecordSeq(data: Future[Seq[Record]], timeout: Duration): FutureRelation =
    apply(data.map(d => TransientRelation(d)), timeout)

  def apply(data: Future[Relation]): FutureRelation = apply(data, defaultTimeout)
  def apply(data: Future[Relation], timeout: Duration) = new FutureRelationImpl(data, timeout)


  private[FutureRelation] class FutureRelationImpl(pData: Future[Relation], defaultTimeout: Duration) extends FutureRelation {

    private val data: Future[Relation] = pData

    /**
      * Blocks until all Futures are complete
      * and afterwards returns the wrapped relations columns.
      *
      * @note behaves the same way as: `val res: Set[UntypedColumnDef] = scala.concurrent.Await.result(FutureRelation.columns)`
      * @return a set of UntypedColumnDefs
      * @see [[scala.concurrent.Await#result]]
      */
    override val columns: Set[UntypedColumnDef] =
      scala.concurrent.Await.result(data, defaultTimeout).columns

    /** @inheritdoc */
    override def where[T](f: (ColumnDef[T], T => Boolean)): FutureRelation =
      FutureRelation(data.map(_.where(f)))

    /** @inheritdoc */
    override def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): FutureRelation =
      FutureRelation(data.map(_.whereAll(fs)))

    /** @inheritdoc */
    override def project(columnDefs: Set[UntypedColumnDef]): FutureRelation =
      FutureRelation(data.map(_.project(columnDefs)))

    /** @inheritdoc */
    override def innerJoin(other: Relation, on: Relation.RecordComparator): FutureRelation =
      futureCheckedBinaryTransformation(other, (rel1, rel2) => rel1.innerJoin(rel2, on))

    /** @inheritdoc */
    override def outerJoin(other: Relation, on: Relation.RecordComparator): FutureRelation =
      futureCheckedBinaryTransformation(other, (rel1, rel2) => rel1.outerJoin(rel2, on))

    /** @inheritdoc */
    override def leftJoin(other: Relation, on: Relation.RecordComparator): FutureRelation =
      futureCheckedBinaryTransformation(other, (rel1, rel2) => rel1.leftJoin(rel2, on))

    /** @inheritdoc */
    override def rightJoin(other: Relation, on: Relation.RecordComparator): FutureRelation =
      futureCheckedBinaryTransformation(other, (rel1, rel2) => rel1.rightJoin(rel2, on))

    /** @inheritdoc */
    override def crossJoin[T](other: Relation, on: (ColumnDef[T], ColumnDef[T])): FutureRelation =
      futureCheckedBinaryTransformation(other, (rel1, rel2) => rel1.crossJoin(rel2, on))

    /** @inheritdoc*/
    override def union(other: Relation): Relation =
      futureCheckedBinaryTransformation(other, (rel1, rel2) => rel1.union(rel2))

    /**
      * Blocks until all Futures are complete
      * and afterwards converts this Relation to a sequence of Records.
      *
      * @note behaves the same way as: `val res: Try[ Seq[Record] ] = scala.concurrent.Await.result(futureRelation)`
      * @return a sequence of Records or an error message
      * @see [[scala.concurrent.Await#result]]
      */
    override def records: Try[Seq[Record]] = Try{
      scala.concurrent.Await.result(data, defaultTimeout).records
    }.flatten

    override def ready(atMost: Duration)(implicit permit: CanAwait): FutureRelationImpl.this.type = {
      data.ready(atMost)(permit)
      this
    }

    override def result(atMost: Duration)(implicit permit: CanAwait): Try[Seq[Record]] = data.result(atMost)(permit).records

    override def pipeTo(actor: ActorRef): Unit = {
      akka.pattern.pipe(data).pipeTo(actor)
    }

    override def future: Future[Try[Seq[Record]]] = data.map(_.records)

    override def transform(f: Relation => Relation): FutureRelation =
      FutureRelation(data.map(f))

    private def futureCheckedBinaryTransformation(other: Relation, op: BinRelationOp): FutureRelation =
      FutureRelation(
        other match {
          case fr: FutureRelationImpl => for {
              rel1 <- data
              rel2 <- fr.data
            } yield op(rel1, rel2)

          case otherRel => data.map(rel => op(rel, otherRel))
        }
      )
  }
}

