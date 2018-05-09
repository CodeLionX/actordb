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
  val columns: Seq[UntypedColumnDef] = cells.keys.toSeq

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
  def project(columnDefs: Seq[UntypedColumnDef]): Try[Record] = Try(internal_project(columnDefs))

  /**
    * Iff all columns of the relation are a subset of this record,
    * returns a new record with only the columns of the relation,
    * otherwise returns an error message.
    * @param r Relation to project this Record to
    * @return A new record containing only the specified columns
    */
  def project(r: Relation): Try[Record] = Try(internal_project(r.columns))

  @throws[IncompatibleColumnDefinitionException]
  private def internal_project(columnDefs: Seq[UntypedColumnDef]): Record =
    if(columnDefs.toSet subsetOf columns.toSet)
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
    *     firstnameCol -> "Hans"
    *   )(
    *     ageCol -> 45
    *   )
    *   .withCellContent(lastnameCol -> "")
    *   .build()
    *
    * // is the same:
    * var rb = Record(Seq(firstnameCol, lastnameCol, ageCol))
    * rb = rb(firstnameCol -> "Hans")
    * rb = rb(lastnameCol -> "")
    * rb = rb(ageCol -> 45)
    * val sameRecord = rb.build()
    *
    * assert(record == sameRecord)
    * }}}
    *
    * This call initiates the [[de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilder]] with
    * the column definitions of the corresponding relational schema
    */
  def apply(columnDefs: Seq[UntypedColumnDef]): RecordBuilder = new RecordBuilder(columnDefs, Map.empty)

  /**
    * Builder for a [[de.up.hpi.informationsystems.adbms.definition.Record]].
    * Initiates the record builder with the column definition list the record should comply with.
    * @param columnDefs all columns of the corresponding relational schema
    * @param recordData initial cell contents, usually: `Map.empty`
    */
  class RecordBuilder(columnDefs: Seq[UntypedColumnDef], recordData: Map[UntypedColumnDef, Any]) {

    /**
      * Sets the selected cell's value.
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

}