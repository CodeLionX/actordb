package de.up.hpi.informationsystems.adbms.definition
import de.up.hpi.informationsystems.adbms.Util

import scala.util.Try


private[definition] object TransientRelation {

  def apply(dataTry: Try[Seq[Record]]): TransientRelation = new TransientRelation(dataTry)

  def apply(data: Seq[Record]): TransientRelation = new TransientRelation(Try(data))

}

private[definition] final class TransientRelation(data: Try[Seq[Record]]) extends Relation with Immutable {

  private val internal_data = data.getOrElse(Seq.empty)
  private val isFailure = data.isFailure

  /** @inheritdoc */
  override val columns: Set[UntypedColumnDef] =
    if(data.isFailure || data.get.isEmpty)
      Set.empty
    else
      data.get.head.columns

  /** @inheritdoc */
  override def where[T](f: (ColumnDef[T], T => Boolean)): Relation =
    if(isFailure)
      this
    else
      TransientRelation(internal_data.filter{ record => record.get[T](f._1).exists(f._2) })

  /** @inheritdoc */
  override def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): Relation =
    if(isFailure)
      this
    else
      TransientRelation(
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
      TransientRelation(Try(
        if(columnDefs subsetOf columns)
          internal_data.map(_.project(columnDefs).get)
        else
          throw IncompatibleColumnDefinitionException(s"this relation does not contain all specified columns {$columnDefs}")
      ))

  /** @inheritdoc */
  override def innerJoin(other: Relation, on: Relation.RecordComparator): Relation = {
    if(isFailure)
      this
    else
      TransientRelation(Try(
        for {
          lside <- internal_data
          rside <- other.records.get
          if on(lside, rside)
        } yield rside ++ lside
      ))
  }

  /** @inheritdoc */
  override def leftJoin(other: Relation, on: Relation.RecordComparator): Relation = {
    val empty = Record.empty
    if(isFailure)
      this
    else
      TransientRelation(Try(
        internal_data.flatMap(rec => {
          val res = other.records.get
            .filter(on.curried(rec))
            .map(rside => rside ++ rec)
          if (res.isEmpty)
            Seq(Record(other.columns).build() ++ rec)
          else res
        })
      ))
  }

  /** @inheritdoc */
  override def rightJoin(other: Relation, on: Relation.RecordComparator): Relation = other.leftJoin(this, on)

  /** @inheritdoc */
  override def outerJoin(other: Relation, on: Relation.RecordComparator): Relation = TransientRelation(Try(
    this.leftJoin(other, on).records.get.union(this.rightJoin(other, on).records.get).distinct
  ))

  /** @inheritdoc */
  override def equiJoin[T](other: Relation, on: (ColumnDef[T], ColumnDef[T])): Relation =
    if(isFailure)
      this
    else
      TransientRelation(Try{
        if(!columns.contains(on._1))
          throw IncompatibleColumnDefinitionException(s"this relation does not contain the specified column {${on._1}}")
        else if(!other.columns.contains(on._2))
          throw IncompatibleColumnDefinitionException(s"the other relation does not contain the specified column {${on._2}}")
        else
          internal_data.flatMap(record => {
            val onKey: T = record.get(on._1).get

            val matchingRecords = other.where[T](on._2 -> {
              _ == onKey
            }).records.get
            matchingRecords.map(otherRecord =>
              record ++ otherRecord
            )
          })
      })

  /** @inheritdoc */
  override def union(other: Relation): Relation =
    if(isFailure)
      this
    else
      TransientRelation(Try{
        if(!this.columns.equals(other.columns))
          throw IncompatibleColumnDefinitionException(s"the columns of this and the other relation does not match\nthis: $columns\nother: ${other.columns}")
        else
          internal_data ++ other.records.get
      })

  /** @inheritdoc */
  override def records: Try[Seq[Record]] = data


  /** @inheritdoc */
  override def toString: String = s"${this.getClass.getSimpleName}:\n" + Util.prettyTable(columns, internal_data)

}
