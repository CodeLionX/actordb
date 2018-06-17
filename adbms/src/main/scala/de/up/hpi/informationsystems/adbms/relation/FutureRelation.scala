package de.up.hpi.informationsystems.adbms.relation

import akka.actor.ActorRef
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, UntypedColumnDef}
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation.Relation.RecordComparator

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Awaitable, CanAwait, Future}
import scala.language.postfixOps
import scala.util.Try


trait FutureRelation extends Relation with Immutable with Awaitable[Try[Seq[Record]]] {

  /**
    * Creates objects of type `B` from this Relation using `mapping` and pipes the resulting objects to given Actor
    * `receiver` on successful Future completion.
    *
    * @example{{{
    *          val msgs: Map[Int, Customer.CustomerRequest] = _
    *          val customers: FutureRelation = Dactor.askDactor(system, classOf[Customer], msgs)
    *          customers.pipeAsMessageTo(relation => <SelfCompanion>.UpdateCustomersMsg(relation), self)
    * }}}
    * @param mapping  function used to create messages sent via messaging from this Relation
    * @param receiver ActorRef of actor to pipe resulting messages to
    * @tparam B       type of messages to be created
    */
  def pipeAsMessageTo[B](mapping: Relation => B, receiver: ActorRef): Unit

  /**
    * Returns this FutureRelation's contents wrapped in a Future.
    * @return the Future[Relation] which will contain this FutureRelation records
    */
  def future: Future[Relation]

  /**
    * Creates a new Relation by applying the function `f` to this Relation.
    * @param  f is the function to be applied
    * @return a new FutureRelation with `f` applied
    */
  def transform(f: Relation => Relation): FutureRelation

  /**
    * Creates a new FutureRelation by applying the function `f` to the successful result of this FutureRelation.
    * This is useful specifically in cases where you have to chain the creation of FutureRelations that depend on
    * each other such as in subsequent askDactor calls.
    *
    * @example{{{
    *          val result: FutureRelation = firstFutureRel.flatTransform( first => {
    *            val idFromFirst = // ... get something from the first FutureRelation which is needed for the second
    *            Dactor.askDactor(system, classOf[SomeActor], Map(idFromFirst -> SomeActor.Message.Request())
    *          })
    * }}}
    * @param  f is the function to be applied
    * @return a FutureRelation which will be completed with the application of the function `f`
    */
  def flatTransform(f: Relation => FutureRelation): FutureRelation

  /** @inheritdoc */
  override def where[T](f: (ColumnDef[T], T => Boolean)): FutureRelation

  /** @inheritdoc */
  override def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): FutureRelation

  /** @inheritdoc */
  override def project(columnDefs: Set[UntypedColumnDef]): FutureRelation

  /** @inheritdoc */
  override def innerJoin(other: Relation, on: RecordComparator): FutureRelation

  /** @inheritdoc */
  override def outerJoin(other: Relation, on: RecordComparator): FutureRelation

  /** @inheritdoc */
  override def leftJoin(other: Relation, on: RecordComparator): FutureRelation

  /** @inheritdoc */
  override def rightJoin(other: Relation, on: RecordComparator): FutureRelation

  /** @inheritdoc */
  override def innerEquiJoin[T](other: Relation, on: (ColumnDef[T], ColumnDef[T])): FutureRelation

  /** @inheritdoc */
  override def union(other: Relation): FutureRelation

  /** @inheritdoc */
  override def applyOn[T](col: ColumnDef[T], f: T => T): FutureRelation
}

object FutureRelation {

  private type BinRelationOp = (Relation, Relation) => Relation

  val defaultTimeout: Duration = 5 seconds

  def fromRecordSeq(data: Future[Seq[Record]]): FutureRelation =
    apply(data.map(d => Relation(d)))
  def fromRecordSeq(data: Future[Seq[Record]], timeout: Duration): FutureRelation =
    apply(data.map(d => Relation(d)), timeout)

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
    override def innerJoin(other: Relation, on: RecordComparator): FutureRelation =
      futureCheckedBinaryTransformation(other, (rel1, rel2) => rel1.innerJoin(rel2, on))

    /** @inheritdoc */
    override def outerJoin(other: Relation, on: RecordComparator): FutureRelation =
      futureCheckedBinaryTransformation(other, (rel1, rel2) => rel1.outerJoin(rel2, on))

    /** @inheritdoc */
    override def leftJoin(other: Relation, on: RecordComparator): FutureRelation =
      futureCheckedBinaryTransformation(other, (rel1, rel2) => rel1.leftJoin(rel2, on))

    /** @inheritdoc */
    override def rightJoin(other: Relation, on: RecordComparator): FutureRelation =
      futureCheckedBinaryTransformation(other, (rel1, rel2) => rel1.rightJoin(rel2, on))

    /** @inheritdoc */
    override def innerEquiJoin[T](other: Relation, on: (ColumnDef[T], ColumnDef[T])): FutureRelation =
      futureCheckedBinaryTransformation(other, (rel1, rel2) => rel1.innerEquiJoin(rel2, on))

    /** @inheritdoc*/
    override def union(other: Relation): FutureRelation =
      futureCheckedBinaryTransformation(other, (rel1, rel2) => rel1.union(rel2))

    /** @inheritdoc */
    override def applyOn[T](col: ColumnDef[T], f: T => T): FutureRelation =
      FutureRelation(data.map(_.applyOn(col, f)))

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

    /** @inheritdoc */
    override def pipeAsMessageTo[B](mapping: Relation => B, receiver: ActorRef): Unit = {
      val msg: Future[B] = data.map(mapping)
      akka.pattern.pipe(msg).pipeTo(receiver)
    }

    /** @inheritdoc */
    override def future: Future[Relation] = data

    /** @inheritdoc */
    override def transform(f: Relation => Relation): FutureRelation =
      FutureRelation(data.map(f))

    /** @inheritdoc */
    override def flatTransform(f: Relation => FutureRelation): FutureRelation =
      FutureRelation(data.flatMap(rel => f(rel).future))

    /**
      * Creates a new Relation from `this` and `other` by applying `op` to them.
      * Checks whether `other` is of type FutureRelation or not and resolves `other` and this FutureRelation
      * simultaneously before applying `op` to both. If `other` is not of type FutureRelation `op` is applied to this
      * FutureRelation and `other` upon this FutureRelations successful completion.
      *
      * @param other  Relation to apply BinRelationOp `op` with `this` to
      * @param op     BinRelationOp to apply
      * @return       a new Relation returned from `op` after application to `this` and `other`
      */
    private def futureCheckedBinaryTransformation(other: Relation, op: BinRelationOp): FutureRelation =
      FutureRelation(
        other match {
          case fr: FutureRelation => for {
              rel1 <- data
              rel2 <- fr.future
            } yield op(rel1, rel2)

          case otherRel => data.map(rel => op(rel, otherRel))
        }
      )
  }
}

