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
  override def records: Try[Seq[Record]] = data


  /** @inheritdoc */
  override def toString: String = s"${this.getClass.getSimpleName}:\n" + Util.prettyTable(columns, internal_data)

  /** @inheritdoc */
  override def crossJoin[T](other: Relation, on: (ColumnDef[T], ColumnDef[T])): Relation =
    if(isFailure)
      this
    else
    TransientRelation(Try(
      internal_data.flatMap( record => {
        val onKey: T = record.get(on._1).get

        val matchingRecords = other.where[T](on._2 -> { _ == onKey }).records.get
        matchingRecords.map( otherRecord =>
          record ++ otherRecord
        )
      })
    ))

}
