package de.up.hpi.informationsystems.adbms.definition

import de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilder

trait Relation {

  /**
    * Returns the column definitions of this relation.
    * @return a sequence of column definitions
    */
  def columns: Seq[UntypedColumnDef]

  /**
    * Inserts a [[de.up.hpi.informationsystems.adbms.definition.Record]] into the relation
    * @param record to be inserted
    */
  def insert(record: Record): Unit

  /**
    * Returns all records satisfying the provided condition.
    * @param f tuple of a column definition and a boolean function
    * @tparam T value type of the column
    * @return all records for which the function is true
    */
  def where[T](f: (ColumnDef[T], T => Boolean)): Seq[Record]

  /**
    * Returns all records satisfying all provided conditions.
    * @note This function has no type guarantees!
    * @param fs map of column definitions and functions on the respective column
    * @return all records for which all functions are true
    */
  def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): Seq[Record]


  // this trait comes with this for nothing :)
  /**
    * Returns a new [[de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilder]] initialized with the
    * columns for this relation.
    * @return initialized RecordBuilder
    */
  def newRecord: RecordBuilder = Record(columns)

  /**
    * Inserts all Records into the relation.
    * @param records to be inserted
    */
  def insertAll(records: Seq[Record]): Unit = records.foreach(insert)


}
