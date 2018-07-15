package de.up.hpi.informationsystems.adbms.record

import java.util.Objects

import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, UntypedColumnDef}
import de.up.hpi.informationsystems.adbms.{IncompatibleColumnDefinitionException, record}

import scala.collection.{GenTraversableOnce, MapLike, mutable}
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
    * Returns the cell's value of a specified column.
    * If the column is not part of this record a Exception will be thrown.
    * @note This call is typesafe!
    *
    * @param columnDef typed column definition specifying the column
    * @tparam T type of the cell's value
    * @return the value of the column's cell
    */
  @throws[IncompatibleColumnDefinitionException]
  def get[T](columnDef: ColumnDef[T]): T =
    if(data.contains(columnDef))
      data(columnDef).asInstanceOf[T]
    else
      throw IncompatibleColumnDefinitionException(s"$columnDef is not part of this record!")

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

  override def default(key: UntypedColumnDef): Any = key.default

  /**
    * Use [[de.up.hpi.informationsystems.adbms.record.Record#get]] instead!
    * It takes care of types!
    */
  @deprecated
  override def get(key: UntypedColumnDef): Option[Any] = Try(get[Any](key.asInstanceOf[ColumnDef[Any]])).toOption

  override def iterator: Iterator[(UntypedColumnDef, Any)] = data.iterator

  override def +[V1 >: Any](kv: (UntypedColumnDef, V1)): Record = new Record(data.+(kv))

  override def -(key: UntypedColumnDef): Record = new Record(data.-(key))

  override def ++[V1 >: Any](xs: GenTraversableOnce[(UntypedColumnDef, V1)]): Record = new Record(data.++(xs))

  override def --(xs: GenTraversableOnce[UntypedColumnDef]): Record = new Record(data.--(xs))

  /** A new record containing the updated column/value mapping.
    *  @param    column the key
    *  @param    value the value
    *  @return   A new record with the new column/value mapping
    */
  override def updated [V1 >: Any](column: UntypedColumnDef, value: V1): Record = this + ((column, value))

  // from Iterable
  override def seq: Map[UntypedColumnDef, Any] = data.seq

  // from Object
  override def toString: String = s"Record($data)"

  override def hashCode(): Int = Objects.hash(columns, data)

  override def canEqual(o: Any): Boolean = o.isInstanceOf[Record]

  override def equals(o: Any): Boolean = o match {
    case that: Record => that.canEqual(this) && this.columns.equals(that.columns) && this.data.equals(that.data)
    case _ => false
  }

  // FIXME: I don't know what to do here.
  // removing this line leads to a compiler error
  override protected def newBuilder: mutable.Builder[(UntypedColumnDef, Any), Record] = ???
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

  private[adbms] def fromMap(columnDefMap: Map[UntypedColumnDef, Any]) = new Record(columnDefMap)

  val empty: Record = new Record(Map.empty)

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
        Record.empty
      else {
        val data: Map[UntypedColumnDef, Any] = columnDefs
          .map{ colDef => Map(colDef -> recordData.getOrElse(colDef, colDef.default)) }
          .reduce( _ ++ _)
        new Record(data)
      }
  }
}