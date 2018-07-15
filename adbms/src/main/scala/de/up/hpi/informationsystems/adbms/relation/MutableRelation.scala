package de.up.hpi.informationsystems.adbms.relation

import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, UntypedColumnDef}
import de.up.hpi.informationsystems.adbms.record.{ColumnCellMapping, Record}

import scala.util.Try

object MutableRelation {
  object BinOps {
    def innerJoin(relation1: MutableRelation, relation2: Relation, on: Relation.RecordComparator): Relation =
      relation1.immutable.innerJoin(relation2, on)

    def outerJoin(relation1: MutableRelation, relation2: Relation, on: Relation.RecordComparator): Relation =
      relation1.immutable.outerJoin(relation2, on)

    def leftJoin(relation1: MutableRelation, relation2: Relation, on: Relation.RecordComparator): Relation =
      relation1.immutable.leftJoin(relation2, on)

    def rightJoin(relation1: MutableRelation, relation2: Relation, on: Relation.RecordComparator): Relation =
      relation1.rightJoin(relation2, on)

    def innerEquiJoin[T](relation1: MutableRelation, relation2: Relation, on: (ColumnDef[T], ColumnDef[T])): Relation =
      relation1.immutable.innerEquiJoin(relation2, on)

    def union(relation1: MutableRelation, relation2: Relation): Relation =
      relation1.immutable.union(relation2)

    def unionAll(relation1: MutableRelation, relation2: Relation): Relation =
      relation1.immutable.unionAll(relation2)
  }
}
trait MutableRelation extends Relation {

  /**
    * Inserts a [[de.up.hpi.informationsystems.adbms.record.Record]] into the relation
    *
    * @param record to be inserted
    * @return the record or an exception wrapped in a Try
    */
  def insert(record: Record): Try[Record]

  /**
    * Deletes the specified record from the relation and returns it.
    * @param record to be deleted
    * @return the record or an exception wrapped in a Try
    */
  def delete(record: Record): Try[Record]

  /**
    * Performs an update to new values for all records satisfying the conditions.
    * Hidden update call, wrapped by the fluent API.
    * @param updateData column-cell-mapping representing the updated values
    * @param fs conditions, which should be met by the records to update
    * @return the number of changed records or an exception if the operation could not be performed
    */
  protected def internalUpdateByWhere(updateData: Map[UntypedColumnDef, Any], fs: Map[UntypedColumnDef, Any => Boolean]): Try[Int]

  //  protected def internalUpdateByKey[T](updateData: Map[UntypedColumnDef, Any], keyValue: Any): Try[Int]


  // implement this trait and get the following for free :)
  /**
    * Updates all records in this relation matching a supplied condition to the new column-cell-mappings.
    * Don't forget to import [[de.up.hpi.informationsystems.adbms.record.ColumnCellMapping]].
    *
    * @example {{{
    *   import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
    *   val result: Try[Int] = Relation
    *       .update(ColumnDef[String]("firstname") ~> "Hans"
    *               & ColumnDef[String]("lastname") ~> "Schmidt")
    *       .where(ColumnDef[Int]("id") -> { _ == 12 })
    * }}}
    * @param mapping new values for the specified columns
    * @return an [[de.up.hpi.informationsystems.adbms.relation.MutableRelation.UpdateBuilder]] to construct the update query
    */
  def update(mapping: ColumnCellMapping): UpdateBuilder = new UpdateBuilder(mapping.toMap)

  /**
    * Inserts all Records into the relation.
    * @note that this is not atomic
    * @param records to be inserted
    */
  // FIXME: insertAll is not atomic and insertions before a possible failure will stay in the relation
  def insertAll(records: Seq[Record]): Try[Seq[Record]] = Try(records.map(r => insert(r).get))

  /**
    * Returns an immutable copy of this relation.
    * @return an [[de.up.hpi.informationsystems.adbms.relation.TransientRelation]] as an immutable copy of this relation
    */
  def immutable: Relation

  // helper
  /**
    * Helps building conditions for updating relations.
    *
    * Part of the fluent relation API.
    * @param updateData column-cell-mappings, which represent the updated data
    */
  class UpdateBuilder private[MutableRelation](updateData: Map[UntypedColumnDef, Any]) {

    //    def byKey[T](keyValue: T): Try[Int] = internalUpdateByKey[T](updateData, keyValue)

    /**
      * Returns the number of records changed by this update operation.
      * @param f tuple of a column definition and a boolean function to restrict update
      * @tparam T value type of the column
      * @return the number of updated records or an exception
      */
    def where[T <: Any](f: (ColumnDef[T], T => Boolean)): Try[Int] =
      internalUpdateByWhere(updateData, Map(f._1.untyped -> { value: Any => f._2(value.asInstanceOf[T]) }))

    /**
      * Returns the number of records changed by this update operation.
      * @note This function has no type guarantees!
      * @param fs map of column definitions and functions on the respective column to restrict update
      * @return the number of updated records or an exception
      */
    def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): Try[Int] =
      internalUpdateByWhere(updateData, fs)
  }

}