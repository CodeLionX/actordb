package de.up.hpi.informationsystems.adbms.definition

import java.util.Objects

import scala.collection.{MapLike, mutable}
import scala.util.Try

class Record private (cells: Map[UntypedColumnDef, Any])
  extends MapLike[UntypedColumnDef, Any, Record]
    with Map[UntypedColumnDef, Any] {

  private val data = cells

  /**
    * Returns column definitions in this record.
    * Alias to `keys`
    */
  val columns: Set[UntypedColumnDef] = cells.keys.toSet

  /**
    * Optionally returns the cell's value of a specified column.
    * @note This call is typesafe!
    * @param columnDef typed column definition specifying the column
    * @tparam T type of the cell's value
    * @return the value of the column's cell wrapped in an `Option`
    */
  def get[T](columnDef: ColumnDef[T]): Option[T] =
    if(data.contains(columnDef))
      Option(data(columnDef).asInstanceOf[T])
    else
      None

  /**
    * Iff `columnDefs` is a subset of this record,
    * performs a projection of this record to the specified columns,
    * or returns an error message.
    * @param columnDefs columns to project to
    * @return A new record containing only the specified columns
    */
  def project(columnDefs: Set[UntypedColumnDef]): Try[Record] = Try(internal_project(columnDefs))

  @throws[IncompatibleColumnDefinitionException]
  private def internal_project(columnDefs: Set[UntypedColumnDef]): Record =
    if(columnDefs subsetOf columns)
      new Record(data.filterKeys(columnDefs.contains))
    else
      throw IncompatibleColumnDefinitionException(s"this record does not contain all specified columns {$columnDefs}")

  // from MapLike
  override def empty: Record = new Record(Map.empty)

  override def default(key: UntypedColumnDef): Any = null

  /**
    * Use [[de.up.hpi.informationsystems.adbms.definition.Record#get]] instead!
    * It takes care of types!
    */
  @Deprecated
  override def get(key: UntypedColumnDef): Option[Any] = get(key.asInstanceOf[ColumnDef[Any]])

  override def iterator: Iterator[(UntypedColumnDef, Any)] = data.iterator

  override def +[V1 >: Any](kv: (UntypedColumnDef, V1)): Map[UntypedColumnDef, V1] = data.+(kv)

  override def -(key: UntypedColumnDef): Record = new Record(data - key)

  // from Iterable
  override def seq: Map[UntypedColumnDef, Any] = data.seq

  // from Object
  override def toString: String = s"Record($data)"

  override def hashCode(): Int = Objects.hash(columns, data)

  override def equals(o: scala.Any): Boolean =
    if (o == null || getClass != o.getClass)
      false
    else {
      // cast other object
      val otherRecord: Record = o.asInstanceOf[Record]
      if (this.columns.equals(otherRecord.columns) && this.data.equals(otherRecord.data))
        true
      else
        false
    }

  // FIXME: I don't know what to do here.
  // removing this line leads to a compiler error
  override protected[this] def newBuilder: mutable.Builder[(UntypedColumnDef, Any), Record] = ???
}

object Record {
  /**
    * Creates a [[de.up.hpi.informationsystems.adbms.definition.Record]] with the builder pattern.
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
    * This call initiates the [[de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilder]] with
    * the column definitions of the corresponding relational schema
    */
  def apply(columnDefs: Set[UntypedColumnDef]): RecordBuilder = new RecordBuilder(columnDefs, Map.empty)

  /**
    * Builder for a [[de.up.hpi.informationsystems.adbms.definition.Record]].
    * Initiates the record builder with the column definition list the record should comply with.
    * @param columnDefs all columns of the corresponding relational schema
    * @param recordData initial cell contents, usually: `Map.empty`
    */
  class RecordBuilder(columnDefs: Set[UntypedColumnDef], recordData: Map[UntypedColumnDef, Any]) {

    /**
      * Sets a cell's value using `colDef ~> &lt;value&gt;` -notation
      * @param in mapping from column to cell content
      * @return the updated [[RecordBuilder]]
      */
    def apply(in: RecordBuilderPart): RecordBuilder =
      new RecordBuilder(columnDefs, recordData ++ in.columnCellMapping)

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
      * Builds the [[de.up.hpi.informationsystems.adbms.definition.Record]] instance.
      * @return a new record
      */
    def build(): Record =
      if(columnDefs.isEmpty)
        new Record(Map.empty)
      else {
        val data: Map[UntypedColumnDef, Any] = columnDefs
          .map{ colDef => Map(colDef -> recordData.getOrElse(colDef, null)) }
          .reduce( _ ++ _)
        new Record(data)
      }
  }

  final class RecordBuilderPart protected[Record](protected[Record] val columnCellMapping: Map[UntypedColumnDef, Any]){
    def +(other: RecordBuilderPart): RecordBuilderPart = and(other)
    def &(other: RecordBuilderPart): RecordBuilderPart = and(other)
    def and(other: RecordBuilderPart): RecordBuilderPart =
      new RecordBuilderPart(this.columnCellMapping ++ other.columnCellMapping)
  }

  /**
    * Provides implicits for dealing with [[de.up.hpi.informationsystems.adbms.definition.Record]]s.
    */
  object implicits {
    implicit class ColumnCellMapper[T](in: ColumnDef[T]) {

      /**
        * Syntax-sugar for creating a mapping of column definition and cell value for the use in
        * [[de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilder]]. Returns a
        * [[de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilderPart]].
        * @param value cell value
        * @return a new [[de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilderPart]] containing
        *         the column definition and cell value mapping
        */
      def ~>(value: T): RecordBuilderPart = new RecordBuilderPart(Map(in.untyped -> value))
    }
  }
}