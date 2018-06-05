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
    * Returns a new Relation only containing the records satisfying the provided condition.
    * @param f tuple of a column definition and a boolean function
    * @tparam T value type of the column
    * @return a new Relation only containing the data satisfying the condition
    */
  def where[T](f: (ColumnDef[T], T => Boolean)): Relation

  /**
    * Returns a new Relation only containing the records satisfying all provided conditions.
    * @note This function has no type guarantees!
    * @param fs map of column definitions and functions on the respective column
    * @return a new Relation only containing the records for which all functions are true
    */
  def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): Relation

  /**
    * Iff `columnDefs` is a subset of this relation's column definition set,
    * performs a projection of this relation to the specified columns,
    * or returns an error message.
    * @param columnDefs columns to project to
    * @return A new Relation containing all records pruned to the specified columns
    */
  def project(columnDefs: Set[UntypedColumnDef]): Relation

  // FIXME: Add scaladoc
  /**
    * @note wip
    */
  def innerJoin(other: Relation, on: (Record, Record) => Boolean): Relation

  // FIXME: Add scaladoc
  /**
    * @note wip
    */
  def outerJoin(other: Relation, on: (Record, Record) => Boolean): Relation

  // FIXME: Add scaladoc
  /**
    * @note wip
    */
  def leftJoin(other: Relation, on: (Record, Record) => Boolean): Relation

  // FIXME: Add scaladoc
  /**
    * @note wip
    */
  def rightJoin(other: Relation, on: (Record, Record) => Boolean): Relation

  /**
    * Joins this relation with another one on the specified columns.
    *
    * @note Currently the column types of the join columns must be the same!
    * @param other
    * @param on
    * @tparam T
    * @return
    */
  def crossJoin[T](other: Relation, on: (ColumnDef[T], ColumnDef[T])): Relation

  /**
    * Converts this Relation to a sequence of Records.
    * @note Depending on the underlying Relation, this operation can be very costly!
    * @return a sequence of Records if all
    */
  def records: Try[Seq[Record]]

  // this trait comes with this for nothing :)
  /**
    * Returns a new [[de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilder]] initialized with the
    * columns for this relation.
    * @return initialized RecordBuilder
    */
  def newRecord: RecordBuilder = Record(columns)

}