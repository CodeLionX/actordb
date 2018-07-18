package de.up.hpi.informationsystems.adbms.relation

import java.util.Objects

import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, UntypedColumnDef}
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.{IncompatibleColumnDefinitionException, Util}

import scala.util.Try

private object TransientRelation {
  object BinOps {
    private type PartialMatcher = PartialFunction[(TransientRelation, TransientRelation), Relation]

    def innerJoin(left: TransientRelation, right: TransientRelation, on: Relation.RecordComparator): Relation =
      propagateFailure.orElse({
        case (l, r) => Relation(Try(
          for {
            lside <- l.internal_data
            rside <- r.internal_data
            if on(lside, rside)
          } yield rside ++ lside
        ))
      }: PartialMatcher).apply(left, right)

    def leftJoin(left: TransientRelation, right: TransientRelation, on: Relation.RecordComparator): Relation =
      propagateFailure.orElse({
        case (l, r) => Relation(Try(
          l.internal_data.flatMap(rec => {
            val res = r.internal_data
              .filter(on.curried(rec))
              .map(rside => rside ++ rec)
            if (res.isEmpty)
              Seq(Record(right.columns).build() ++ rec)
            else res
          })
        ))
      }: PartialMatcher).apply(left, right)

    def rightJoin(left: TransientRelation, right: TransientRelation, on: Relation.RecordComparator): Relation =
      leftJoin(right, left, on)

    def outerJoin(left: TransientRelation, right: TransientRelation, on: Relation.RecordComparator): Relation =
      union(leftJoin(left, right, on).asInstanceOf[TransientRelation], rightJoin(left, right, on).asInstanceOf[TransientRelation])

    def innerEquiJoin[T](left: TransientRelation, right: TransientRelation, on: (ColumnDef[T], ColumnDef[T])): Relation =
      propagateFailure.orElse({
        case (l, r) => Relation(Try {
          throwErrorIfColumnNotIn(l.columns, on._1)
          throwErrorIfColumnNotIn(r.columns, on._2)

          val recMap = r.internal_data.groupBy(_.get(on._2))
          l.internal_data.flatMap(record => {
            val matchingRecords = recMap.getOrElse(record.get(on._1), Seq.empty)
            matchingRecords.map(otherRecord =>
              record ++ otherRecord
            )
          })
        })
      }: PartialMatcher).apply(left, right)

    def unionAll(left: TransientRelation, right: TransientRelation): Relation =
      propagateFailure.orElse({
        case (l, r) => Relation(Try {
          throwErrorIfColumnsNotEqual(l.columns, r.columns)
          l.internal_data ++ r.internal_data
        })
      }: PartialMatcher).apply(left, right)

    def union(left: TransientRelation, right: TransientRelation): Relation =
      propagateFailure.orElse({
        case (l, r) => Relation(Try {
          throwErrorIfColumnsNotEqual(l.columns, r.columns)
          (l.internal_data ++ r.internal_data).distinct
        })
      }: PartialMatcher).apply(left, right)

    def propagateFailure: PartialMatcher = {
      case (left, _)  if left.isFailure  => left
      case (_, right) if right.isFailure => right
    }

    @throws[IncompatibleColumnDefinitionException]
    def throwErrorIfColumnsNotEqual(cols1: Set[UntypedColumnDef], cols2: Set[UntypedColumnDef]): Unit = {
      if (!cols1.equals(cols2))
        throw IncompatibleColumnDefinitionException(
          s"""the columns of this and the other relation do not match
             |left: $cols1
             |right: $cols2
             |""".stripMargin)
    }

    @throws[IncompatibleColumnDefinitionException]
    def throwErrorIfColumnNotIn(cols: Set[UntypedColumnDef], column: UntypedColumnDef): Unit = {
      if (!cols.contains(column))
        throw IncompatibleColumnDefinitionException(
          s"""the relation with columns:
             |$cols
             |does not contain the specified column: $column
             |""".stripMargin)
    }
  }
}

private[relation] final class TransientRelation(data: Try[Seq[Record]]) extends Relation with Immutable {

  private val internal_data = data.getOrElse(Seq.empty)
  private val isFailure = data.isFailure

  /** @inheritdoc */
  override val columns: Set[UntypedColumnDef] =
    if(data.isFailure || data.get.isEmpty)
      Set.empty
    else
      data.get.head.columns

  override def hashCode(): Int = Objects.hashCode(internal_data, isFailure, columns)

  def canEqual(o: Any): Boolean = o.isInstanceOf[TransientRelation]

  override def equals(obj: Any): Boolean = obj match {
    case that: TransientRelation => that.canEqual(this) && this.hashCode() == that.hashCode()
    case _ => false
  }


  /** @inheritdoc */
  override def where[T](f: (ColumnDef[T], T => Boolean)): Relation =
    if(isFailure)
      this
    else
      Relation(internal_data.filter{ record => record.get[T](f._1).exists(f._2) })

  /** @inheritdoc */
  override def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): Relation =
    if(isFailure)
      this
    else
      Relation(
        // filter all records
        internal_data.filter{ record =>
          fs.keys
            // map over all supplied filters (key = column)
            .map { col: UntypedColumnDef =>
              /* `val rVal = record(col)` returns the value in the record for the column `col`
               * `val filterF = fs(col)` returns the filter for column `col`
               * `val res = filterF(rVal)` applies the filter to the value of the record and corresponding column,
               * returning `true` or `false`
               */
              fs(col)(record(col))
            }
            // test if all filters for this record are true
            .forall(_ == true)
        }
      )

  /** @inheritdoc */
  override def project(columnDefs: Set[UntypedColumnDef]): Relation =
    if(isFailure)
      this
    else
      Relation(Try(
        if(columnDefs subsetOf columns)
          internal_data.map(_.project(columnDefs).get)
        else
          throw IncompatibleColumnDefinitionException(s"this relation does not contain all specified columns {$columnDefs}")
      ))

  /** @inheritdoc*/
  override def applyOn[T](col: ColumnDef[T], f: T => T): Relation =
    if(isFailure || !Set(col).subsetOf(columns))
      this
    else
      Relation(Try{
        internal_data.map( record => record.get(col) match {
          case Some(value) =>
            val newValue = f(value)
            record.updated(col, newValue)
          case None => record
        })
      })

  /** @inheritdoc */
  override def records: Try[Seq[Record]] = data


  /** @inheritdoc */
  override def toString: String = s"${this.getClass.getSimpleName}:\n" + Util.prettyTable(columns, internal_data)

}
