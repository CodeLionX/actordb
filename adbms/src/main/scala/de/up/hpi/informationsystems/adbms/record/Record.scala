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
//class Record private (cells: Map[UntypedColumnDef, Any])
//  extends MapLike[UntypedColumnDef, Any, Record]
//    with Map[UntypedColumnDef, Any] {
//
//  private val data = cells
//
//  /**
//    * Returns column definitions in this record.
//    * Alias to `keys`
//    */
//  val columns: Set[UntypedColumnDef] = cells.keys.toSet
//
//  /**
//    * Optionally returns the cell's value of a specified column.
//    * @note This call is typesafe!
//    * @param columnDef typed column definition specifying the column
//    * @tparam T type of the cell's value
//    * @return the value of the column's cell wrapped in an `Option`
//    */
//  def get[T](columnDef: ColumnDef[T]): Option[T] =
//    if(data.contains(columnDef))
//      Option(data(columnDef).asInstanceOf[T])
//    else
//      None
//
//  /**
//    * Iff `columnDefs` is a subset of this record,
//    * performs a projection of this record to the specified columns,
//    * or returns an error message.
//    * @param columnDefs columns to project to
//    * @return A new record containing only the specified columns
//    */
//  def project(columnDefs: Set[UntypedColumnDef]): Try[Record] = Try(internal_project(columnDefs))
//
//  @throws[IncompatibleColumnDefinitionException]
//  private def internal_project(columnDefs: Set[UntypedColumnDef]): Record =
//    if(columnDefs subsetOf columns)
//      new Record(data.filterKeys(columnDefs.contains))
//    else
//      throw IncompatibleColumnDefinitionException(s"this record does not contain all specified columns {$columnDefs}")
//
//  // from MapLike
//  override def empty: Record = new Record(Map.empty)
//
//  override def default(key: UntypedColumnDef): Any = null
//
//  /**
//    * Use [[de.up.hpi.informationsystems.adbms.record.Record#get]] instead!
//    * It takes care of types!
//    */
//  @deprecated
//  override def get(key: UntypedColumnDef): Option[Any] = get(key.asInstanceOf[ColumnDef[Any]])
//
//  override def iterator: Iterator[(UntypedColumnDef, Any)] = data.iterator
//
//  override def +[V1 >: Any](kv: (UntypedColumnDef, V1)): Record = new Record(data.+(kv))
//
//  override def -(key: UntypedColumnDef): Record = new Record(data.-(key))
//
//  override def ++[V1 >: Any](xs: GenTraversableOnce[(UntypedColumnDef, V1)]): Record = new Record(data.++(xs))
//
//  override def --(xs: GenTraversableOnce[UntypedColumnDef]): Record = new Record(data.--(xs))
//
//  /** A new record containing the updated column/value mapping.
//    *  @param    column the key
//    *  @param    value the value
//    *  @return   A new record with the new column/value mapping
//    */
//  override def updated [V1 >: Any](column: UntypedColumnDef, value: V1): Record = this + ((column, value))
//
//  // from Iterable
//  override def seq: Map[UntypedColumnDef, Any] = data.seq
//
//  // from Object
//  override def toString: String = s"Record($data)"
//
//  override def hashCode(): Int = Objects.hash(columns, data)
//
//  override def canEqual(o: Any): Boolean = o.isInstanceOf[Record]
//
//  override def equals(o: Any): Boolean = o match {
//    case that: Record => that.canEqual(this) && this.columns.equals(that.columns) && this.data.equals(that.data)
//    case _ => false
//  }
//
//  // FIXME: I don't know what to do here.
//  // removing this line leads to a compiler error
//  override protected def newBuilder: mutable.Builder[(UntypedColumnDef, Any), Record] = ???
//}

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