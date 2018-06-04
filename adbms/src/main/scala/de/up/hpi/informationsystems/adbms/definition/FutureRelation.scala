package de.up.hpi.informationsystems.adbms.definition

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Awaitable, CanAwait, Future}
import scala.language.postfixOps
import scala.util.Try


trait FutureRelation extends Relation with Immutable with Awaitable[Try[Seq[Record]]]

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

    /** @inheritdoc */
    override val columns: Set[UntypedColumnDef] = Set.empty // FIXME: find a good way to represent this

    /** @inheritdoc */
    override def where[T](f: (ColumnDef[T], T => Boolean)): Relation =
      FutureRelation(data.map(_.where(f)))

    /** @inheritdoc */
    override def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): Relation =
      FutureRelation(data.map(_.whereAll(fs)))

    /** @inheritdoc */
    override def project(columnDefs: Set[UntypedColumnDef]): Relation =
      FutureRelation(data.map(_.project(columnDefs)))

    /** @inheritdoc */
    // FIXME NOT IMPLEMENTED
    override def innerJoin(other: Relation, on: (Record, Record) => Boolean): Relation = ???

    /** @inheritdoc */
    // FIXME NOT IMPLEMENTED
    override def outerJoin(other: Relation, on: (Record, Record) => Boolean): Relation = ???

    /** @inheritdoc */
    // FIXME NOT IMPLEMENTED
    override def leftJoin(other: Relation, on: (Record, Record) => Boolean): Relation = ???

    /** @inheritdoc */
    // FIXME NOT IMPLEMENTED
    override def rightJoin(other: Relation, on: (Record, Record) => Boolean): Relation = ???
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
  }
}

