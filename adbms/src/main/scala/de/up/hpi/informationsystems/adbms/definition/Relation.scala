package de.up.hpi.informationsystems.adbms.definition

import de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilder

import scala.util.Try

object Relation {
  type RecordComparator = (Record, Record) => Boolean

  def apply(dataTry: Try[Seq[Record]]): Relation = new TransientRelation(dataTry)

  def apply(data: Seq[Record]): Relation = new TransientRelation(Try(data))
}

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

  /**
    * Performs an inner join with another relation on a comparator function.
    *
    * @param other relation to join with
    * @param on `RecordComparator`, i.e. (Record, Record) => Boolean, which determines which
    *          pairs of records from the respective relations are in the result set. The
    *          result set contains exactly the records made up of the pairs of records for
    *          which `on` returns `true`.
    * @return a new relation comprised of the joined records from `this` and `other`
    */
  def innerJoin(other: Relation, on: Relation.RecordComparator): Relation

  /**
    * Performs an outer join with another relation on a comparator function.
    * @param other relation to join with
    * @param on `RecordComparator`, i.e. (Record, Record) => Boolean, which determines which
    *          pairs of records from the respective relations are in the result set. The
    *          result set contains exactly the records made up of the pairs of records for
    *          which `on` returns `true`.
    * @return a new relation comprised of the joined records from `this` and `other`
    */
  def outerJoin(other: Relation, on: Relation.RecordComparator): Relation

  /**
    * Performs an left join with another relation on a comparator function.
    * @param other relation to join with
    * @param on `RecordComparator`, i.e. (Record, Record) => Boolean, which determines which
    *          pairs of records from the respective relations are in the result set. The
    *          result set contains exactly the records made up of the pairs of records for
    *          which `on` returns `true`.
    * @return a new relation comprised of the joined records from `this` and `other`
    */
  def leftJoin(other: Relation, on: Relation.RecordComparator): Relation

  /**
    * Performs an right join with another relation on a comparator function.
    * @param other relation to join with
    * @param on `RecordComparator`, i.e. (Record, Record) => Boolean, which determines which
    *          pairs of records from the respective relations are in the result set. The
    *          result set contains exactly the records made up of the pairs of records for
    *          which `on` returns `true`.
    * @return a new relation comprised of the joined records from `this` and `other`
    */
  def rightJoin(other: Relation, on: Relation.RecordComparator): Relation

  /**
    * Performs an equality join of this relation with another one on the specified columns.
    *
    * @note Currently the column types of the join columns must be the same!
    * @param other relation to join with
    * @param on tuple of `ColumnDef[T]`s which determine which columns' attributes are
    *           compared for the equality join
    * @tparam T
    * @return a new relation comprised of the joined records from `this` and `other`
    */
  def innerEquiJoin[T](other: Relation, on: (ColumnDef[T], ColumnDef[T])): Relation

  /**
    * Performs a union with another relation, iff the relations have the same schema definition.
    * @param other relation to join with
    * @return a new relation containing the records from both relations
    */
  def union(other: Relation): Relation

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