package de.up.hpi.informationsystems.adbms
package definition

private[definition] object ColumnStore {
  def apply[T](columnDef: TypedColumnDef[T]): TypedColumnStore[T] = new TypedColumnStore[T](columnDef)
}

/**
  * Represents one Column with stored values of a column-oriented store.
  * Values are stored in order.
  */
private[definition] sealed trait ColumnStore {
  /**
    * Type of the values hold by this Column
    */
  type valueType

  /**
    * Returns the column definition of this Column
    * @return column definition
    */
  def columnDef: ColumnDef

  /**
    * Appends a new value at the end of the Column's store.
    * @param value to be appended
    */
  def append(value: valueType): Unit

  /**
    * Returns the value at position (row) <code>idx</code>.
    * @param idx index / row number beginning from 0
    * @return the value at the index <code>idx</code>
    */
  def get(idx: Int): Option[valueType]

  /**
    * Returns the number of cells.
    * @return number of cells
    */
  def length: Int
}

/**
  * A Column with a fixed Type T
  * @param columnDefInt typed column definition
  * @tparam T type of the values hold by this column
  */
private[definition] class TypedColumnStore[T](columnDefInt: TypedColumnDef[T]) extends ColumnStore {
  override type valueType = T
  override def columnDef: ColumnDef = columnDefInt
  private var data: Seq[T] = Seq.empty

  override def append(value: T): Unit =
    data = data :+ value

  override def get(idx: Int): Option[T] =
    if(idx < data.length && idx >= 0)
      Some(data(idx))
    else
      None

  override def length: Int = data.length

  override def toString: String = s"TypedColumn[${columnDef.tpe}]($data)"
}
