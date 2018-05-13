package de.up.hpi.informationsystems.adbms.definition

import de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilder

import scala.util.Try

trait Relation {

  /**
    * Returns the column definitions of this relation.
    * @note override this value to define your relational schema
    * @return a sequence of column definitions
    */
  val columns: Set[UntypedColumnDef]

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

  /**
    * Iff `columnDefs` is a subset of this relation's column definition set,
    * performs a projection of this relation to the specified columns,
    * or returns an error message.
    * @param columnDefs columns to project to
    * @return All records containing only the specified columns
    */
  def project(columnDefs: Set[UntypedColumnDef]): Try[Seq[Record]]


  // this trait comes with this for nothing :)
  /**
    * Returns a new [[de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilder]] initialized with the
    * columns for this relation.
    * @return initialized RecordBuilder
    */
  def newRecord: RecordBuilder = Record(columns)


  /**
    * Inserts all Records into the relation.
    * @note that this is not atomic
    * @param records to be inserted
    */
  // FIXME: insertAll is not atomic and insertions before a possible failure will stay in the relation
  def insertAll(records: Seq[Record]): Try[Seq[Record]] = Try(records.map(r => insert(r).get))
}

trait MutableRelation extends Relation {

  /**
    * Inserts a [[de.up.hpi.informationsystems.adbms.definition.Record]] into the relation
    * @param record to be inserted
    */
  def insert(record: Record): Try[Record]

}