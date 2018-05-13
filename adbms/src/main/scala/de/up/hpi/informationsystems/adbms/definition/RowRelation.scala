package de.up.hpi.informationsystems.adbms.definition

import de.up.hpi.informationsystems.adbms.Util

import scala.util.Try

abstract class RowRelation extends MutableRelation {

  private var data: Seq[Record] = Seq.empty

  /** @inheritdoc */
  override def insert(record: Record): Try[Record] = Try(internal_insert(record))

  @throws[IncompatibleColumnDefinitionException]
  private def internal_insert(record: Record): Record =
    // check for correct column layout
    if(record.columns == columns) {
      data = data :+ record
      record
    } else {
      throw IncompatibleColumnDefinitionException(s"this records column layout does not match this " +
      s"relations schema:\n${record.columns} (record)\n${this.columns} (relation)")
    }

  /** @inheritdoc */
  override def where[T](f: (ColumnDef[T], T => Boolean)): Relation = TransientRelation(data).where(f)

  /** @inheritdoc */
  override def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): Relation = TransientRelation(data).whereAll(fs)

  /** @inheritdoc */
  override def project(columnDefs: Set[UntypedColumnDef]): Relation = TransientRelation(data).project(columnDefs)

  /** @inheritdoc */
  override def records: Try[Seq[Record]] = Try(data)

  /** @inheritdoc */
  override def toString: String = s"${this.getClass.getSimpleName}:\n" + Util.prettyTable(columns, data)
}