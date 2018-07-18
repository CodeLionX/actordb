package de.up.hpi.informationsystems.adbms.record

import de.up.hpi.informationsystems.adbms.definition.ColumnDef
import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.record.internal.{TypedMapBase, TypedMapLike}
import de.up.hpi.informationsystems.adbms.{IncompatibleColumnDefinitionException, record}

import scala.language.higherKinds
import scala.util.Try


class Record(override protected val data: Map[ColumnDef[Any], Any])
  extends TypedMapBase[ColumnDef, Any]
    with TypedMapLike[ColumnDef, Any, Record] {

  val columns: Set[ColumnDef[Any]] = data.keySet

  /**
    * Iff `columnDefs` is a subset of this record,
    * performs a projection of this record to the specified columns,
    * or returns an error message.
    * @param columnDefs columns to project to
    * @return A new record containing only the specified columns
    */
  def project(columnDefs: Set[ColumnDef[Any]]): Try[Record] = Try(internal_project(columnDefs))

  private def internal_project(columnDefs: Set[ColumnDef[Any]]): Record =
    if(columnDefs subsetOf columns)
      new Record(data.filterKeys(columnDefs.contains))
    else
      throw IncompatibleColumnDefinitionException(s"this record does not contain all specified columns {$columnDefs}")

  // from TypedMapLike
  override protected def ctor(cells: Map[ColumnDef[Any], Any]): Record = new Record(cells)

}

object Record {
  /**
    * Creates a [[de.up.hpi.informationsystems.adbms.record.Record]] with the builder pattern.
    *
    * @example {{{
    * val firstnameCol = ColumnDef[String]("Firstname")
    * val lastnameCol = ColumnDef[String]("Lastname")
    * val ageCol = ColumnDef[Int]("Age")
    *
    * // syntactic sugar
    * val record = Record(Seq(firstnameCol, lastnameCol, ageCol))(
    *     firstnameCol ~> "Hans" &
    *     ageCol ~> 45 +
    *     lastnameCol -> ""
    *   ).build()
    *
    * // is the same:
    * val sameRecord = Record(Seq(firstnameCol, lastnameCol, ageCol))
    *   .withCellContent(firstnameCol)("Hans")
    *   .withCellContent(lastnameCol)("")
    *   .withCellContent(ageCol)(45)
    *   .build()
    *
    * assert(record == sameRecord)
    * }}}
    *
    * This call initiates the [[record.Record.RecordBuilder]] with
    * the column definitions of the corresponding relational schema
    */
  def apply(columnDefs: Set[UntypedColumnDef]): RecordBuilder = new RecordBuilder(columnDefs, Map.empty)

  private[adbms] def fromMap(columnDefMap: Map[UntypedColumnDef, Any]): Record = new Record(columnDefMap)

  private[adbms] def fromVector(columnDefs: Vector[UntypedColumnDef])(data: Vector[Any]): Record =
    new Record(columnDefs.indices.map{ index => columnDefs(index) -> data(index) }.toMap)

  val empty: Record = Record.empty

  /**
    * Builder for a [[de.up.hpi.informationsystems.adbms.record.Record]].
    * Initiates the record builder with the column definition list the record should comply with.
    *
    * @param columnDefs all columns of the corresponding relational schema
    * @param recordData initial cell contents, usually: `Map.empty`
    */
  class RecordBuilder(columnDefs: Set[UntypedColumnDef], recordData: Map[UntypedColumnDef, Any]) {

    /**
      * Sets a cell's value using `colDef ~> &lt;value&gt;` -notation
      * @param in mapping from column to cell content
      * @return the updated [[RecordBuilder]]
      */
    def apply(in: ColumnCellMapping): RecordBuilder =
      new RecordBuilder(columnDefs, recordData ++ in.toMap)

    /**
      * Sets the selected cell's value.
      * Curried function for having better compiler and IDE type mismatch errors.
      * @param colDef cell to set indicated by column definition
      * @param value value of the cell
      * @tparam T value type, same as for the column definition
      * @return the updated [[RecordBuilder]]
      */
    def withCellContent[T](colDef: ColumnDef[T])(value: T): RecordBuilder =
      new RecordBuilder(columnDefs, recordData ++ Map(colDef -> value))

    /**
      * Builds the [[de.up.hpi.informationsystems.adbms.record.Record]] instance.
      *
      * @return a new record
      */
    def build(): Record =
      if(columnDefs.isEmpty)
        new Record(Map.empty)
      else {
        val data: Map[UntypedColumnDef, Any] = columnDefs
          .map{ colDef => Map(colDef -> recordData.getOrElse(colDef, colDef.default)) }
          .reduce( _ ++ _)
        new Record(data)
      }
  }
}