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
      FutureRelation(data.map(_.innerJoin(other, on)))

    /** @inheritdoc */
    override def outerJoin(other: Relation, on: Relation.RecordComparator): FutureRelation =
      FutureRelation(data.map(_.outerJoin(other, on)))

    /** @inheritdoc */
    override def leftJoin(other: Relation, on: Relation.RecordComparator): FutureRelation =
      FutureRelation(data.map(_.leftJoin(other, on)))

    /** @inheritdoc */
    override def rightJoin(other: Relation, on: Relation.RecordComparator): FutureRelation =
      FutureRelation(data.map(_.rightJoin(other, on)))

    /** @inheritdoc */
    override def crossJoin[T](other: Relation, on: (ColumnDef[T], ColumnDef[T])): FutureRelation =
      FutureRelation(data.map(_.crossJoin(other, on)))

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
  }
}

