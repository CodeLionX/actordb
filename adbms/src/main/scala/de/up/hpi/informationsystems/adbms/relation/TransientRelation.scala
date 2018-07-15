package de.up.hpi.informationsystems.adbms.relation

import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, UntypedColumnDef}
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.{IncompatibleColumnDefinitionException, Util}

import scala.util.Try

private object TransientRelation {
  object BinOps {

    def innerJoin(left: TransientRelation, right: TransientRelation, on: Relation.RecordComparator): Relation =
      if(left.isFailure)
        left
      else if(right.isFailure)
        right
      else
        Relation(Try(
          for {
            lside <- left.internal_data
            rside <- right.internal_data
            if on(lside, rside)
          } yield rside ++ lside
        ))

    def leftJoin(left: TransientRelation, right: TransientRelation, on: Relation.RecordComparator): Relation =
      if(left.isFailure)
        left
      else if(right.isFailure)
        right
      else
        Relation(Try(
          left.internal_data.flatMap(rec => {
            val res = right.internal_data
              .filter(on.curried(rec))
              .map(rside => rside ++ rec)
            if (res.isEmpty)
              Seq(Record(right.columns).build() ++ rec)
            else res
          })
        ))

    def rightJoin(left: TransientRelation, right: TransientRelation, on: Relation.RecordComparator): Relation =
      leftJoin(right, left, on)

    def outerJoin(left: TransientRelation, right: TransientRelation, on: Relation.RecordComparator): Relation =
      union(leftJoin(left, right, on).asInstanceOf[TransientRelation], rightJoin(left, right, on).asInstanceOf[TransientRelation])

    def innerEquiJoin[T](left: TransientRelation, right: TransientRelation, on: (ColumnDef[T], ColumnDef[T])): Relation =
      if(left.isFailure)
        left
      else if(right.isFailure)
        right
      else
        Relation(Try{
          if(!left.columns.contains(on._1))
            throw IncompatibleColumnDefinitionException(s"the left relation does not contain the specified column {${on._1}}")
          else if(!right.columns.contains(on._2))
            throw IncompatibleColumnDefinitionException(s"the right relation does not contain the specified column {${on._2}}")
          else {
            val recMap = right.internal_data.groupBy(_.get(on._2))
            left.internal_data.flatMap(record => {
              val matchingRecords = recMap.getOrElse(record.get(on._1), Seq.empty)
              matchingRecords.map(otherRecord =>
                record ++ otherRecord
              )
            })
          }
        })

    def unionAll(left: TransientRelation, right: TransientRelation): Relation =
      if(left.isFailure)
        left
      else if(right.isFailure)
        right
      else
        Relation(Try{
          if(!left.columns.equals(right.columns))
            throw IncompatibleColumnDefinitionException(s"the columns of this and the other relation does not match\nleft: ${left.columns}\nright: ${right.columns}")
          else
            left.internal_data ++ right.internal_data
        })

    def union(left: TransientRelation, right: TransientRelation): Relation =
      if(left.isFailure)
        left
      else if(right.isFailure)
        right
      else
        Relation(Try{
          if(!left.columns.equals(right.columns))
            throw IncompatibleColumnDefinitionException(s"the columns of this and the other relation does not match\nleft: ${left.columns}\nright: ${right.columns}")
          else
            (left.internal_data ++ right.internal_data).distinct
        })

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

  /** @inheritdoc*/
  override def equals(obj: scala.Any): Boolean =
    obj.isInstanceOf[TransientRelation] &&
      (hashCode() == obj.asInstanceOf[TransientRelation].hashCode())

  override def hashCode(): Int =
    internal_data.hashCode() + isFailure.hashCode() + columns.hashCode()

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
