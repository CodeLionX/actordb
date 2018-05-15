package de.up.hpi.informationsystems.adbms.definition

import scala.util.Try

trait MutableRelation extends Relation {

  /**
    * Inserts a [[de.up.hpi.informationsystems.adbms.definition.Record]] into the relation
    * @param record to be inserted
    */
  def insert(record: Record): Try[Record]

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
    * Don't forget to import [[de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping]].
    *
    * @example {{{
    *   import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
    *   val result: Try[Int] = Relation
    *       .update(ColumnDef[String]("firstname") ~> "Hans"
    *               & ColumnDef[String]("lastname") ~> "Schmidt")
    *       .where(ColumnDef[Int]("id") -> { _ == 12 })
    * }}}
    * @param mapping new values for the specified columns
    * @return an [[de.up.hpi.informationsystems.adbms.definition.MutableRelation.UpdateBuilder]] to construct the update query
    */
  def update(mapping: ColumnCellMapping): UpdateBuilder = new UpdateBuilder(mapping.toMap)

  /**
    * Inserts all Records into the relation.
    * @note that this is not atomic
    * @param records to be inserted
    */
  // FIXME: insertAll is not atomic and insertions before a possible failure will stay in the relation
  def insertAll(records: Seq[Record]): Try[Seq[Record]] = Try(records.map(r => insert(r).get))


  // helper
  /**
    * Helps building conditions for updating relations.
    *
    * Part of the fluent relation API.
    * @param updateData column-cell-mappings, which represent the updated data
    */
  class UpdateBuilder private[MutableRelation] (updateData: Map[UntypedColumnDef, Any]) {

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