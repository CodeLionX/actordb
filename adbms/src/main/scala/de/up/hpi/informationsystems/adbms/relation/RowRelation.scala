package de.up.hpi.informationsystems.adbms.relation

import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation.Relation.RecordComparator
import de.up.hpi.informationsystems.adbms.{IncompatibleColumnDefinitionException, RecordNotFoundException, Util}

import scala.util.Try

object RowRelation {

  /**
    * Creates an instance of a row relation, which actually stores data.
    * Use a [[de.up.hpi.informationsystems.adbms.definition.RelationDef]] to define your relational schema.
    * Use like the following:
    *
    * @example {{{
    *           object MyRelation extends RelationDef {
    *             ...
    *           }
    *           val myRelation: MutableRelation = RowRelation(MyRelation)
    * }}}
    *
    * @see [[de.up.hpi.informationsystems.adbms.definition.RelationDef]]
    */
  def apply(relDef: RelationDef): MutableRelation = new RowRelation(relDef.columns)
}

private final class RowRelation(passedColumns: Set[UntypedColumnDef]) extends MutableRelation {

  private var data: Seq[Record] = Seq.empty

  /** @inheritdoc */
  override val columns: Set[UntypedColumnDef] = passedColumns

  /** @inheritdoc */
  override def insert(record: Record): Try[Record] = Try{
    exceptionWhenNotEqual(record.columns)
    data = data :+ record
    record
  }

  /** @inheritdoc*/
  override def delete(record: Record): Try[Record] = Try{
    exceptionWhenNotEqual(record.columns)
    if(!data.contains(record))
      throw RecordNotFoundException(s"this relation does not contain the record: $record")
    data = data.filterNot(_ == record)
    record
  }

  /** @inheritdoc*/
  override protected def internalUpdateByWhere(
        updateData: Map[UntypedColumnDef, Any], fs: Map[UntypedColumnDef, Any => Boolean]
      ): Try[Int] = Try {
    exceptionWhenNotSubset(updateData.keys)
    var counter = 0
    data = data.map( record => {
      val allFiltersApply = fs.keys
        .map { col: UntypedColumnDef => fs(col)(record(col)) }
        .forall(_ == true)

      if(allFiltersApply){
        counter += 1
        updateData.keys.foldLeft(record)((record, updateCol) => record.updated(updateCol, updateData(updateCol)))
      }
      else
        record
    })
    counter
  }

  /** @inheritdoc */
  override def where[T](f: (ColumnDef[T], T => Boolean)): Relation = Relation(data).where(f)

  /** @inheritdoc */
  override def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): Relation = Relation(data).whereAll(fs)

  /** @inheritdoc */
  override def project(columnDefs: Set[UntypedColumnDef]): Relation = Relation(data).project(columnDefs)

  /** @inheritdoc*/
  override def innerEquiJoin[T](other: Relation, on: (ColumnDef[T], ColumnDef[T])): Relation =
    Relation(data).innerEquiJoin(other, on)

  /** @inheritdoc */
  override def innerJoin(other: Relation, on: RecordComparator): Relation =
    Relation(records).innerJoin(other, on)

  /** @inheritdoc */
  override def outerJoin(other: Relation, on: RecordComparator): Relation =
    Relation(records).outerJoin(other, on)

  /** @inheritdoc */
  override def leftJoin(other: Relation, on: RecordComparator): Relation =
    Relation(records).leftJoin(other, on)

  /** @inheritdoc */
  override def rightJoin(other: Relation, on: RecordComparator): Relation =
    Relation(records).rightJoin(other, on)

  /** @inheritdoc */
  override def union(other: Relation): Relation =
    Relation(records).union(other)

  /** @inheritdoc */
  override def applyOn[T](col: ColumnDef[T], f: T => T): Relation =
    Relation(records).applyOn(col, f)

  /** @inheritdoc */
  override def records: Try[Seq[Record]] = Try(data)

  /** @inheritdoc */
  override def toString: String = s"${this.getClass.getSimpleName}:\n" + Util.prettyTable(columns, data)

  /** @inheritdoc*/
  override def immutable: Relation = Relation(data)

  @throws[IncompatibleColumnDefinitionException]
  private def exceptionWhenNotSubset(incomingColumns: Iterable[UntypedColumnDef]): Unit =
    if (!(incomingColumns.toSet subsetOf columns)) {
      val notMatchingColumns = incomingColumns.toSet -- columns
      throw IncompatibleColumnDefinitionException(s"this relation does not contain following columns: $notMatchingColumns")
    }

  @throws[IncompatibleColumnDefinitionException]
  private def exceptionWhenNotEqual(incomingColumns: Iterable[UntypedColumnDef]): Unit =
    if(incomingColumns != columns)
      throw IncompatibleColumnDefinitionException(s"the provided column layout does not match this " +
        s"relation's schema:\n$incomingColumns (provided)\n${this.columns} (relation)")

}