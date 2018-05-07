package de.up.hpi.informationsystems.adbms
package definition

import scala.collection.mutable

private[definition] object ColumnStore {
  def apply[T](columnDef: ColumnDef[T]): TypedColumnStore[T] = new TypedColumnStore[T](columnDef)
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
  def columnDef: UntypedColumnDef

  /**
    * Appends a new value at the end of the Column's store.
    * @param value to be appended
    */
  def append(value: valueType): Unit


  /**
    * Returns the value at the specified index
    * @param idx position of value
    * @return value at position idx
    */
  def apply(idx: Int): valueType

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
  def size: Int

  def update(idx: Int, elem: valueType): Unit

  def isEmpty: Boolean

  def indexOf[B >: valueType](elem: B): Int

  def indicesWhere(p: valueType => Boolean): Seq[Int]

  def contains[A1 >: valueType](elem: A1): Boolean

  def indices: Range
}

/**
  * A Column with a fixed Type T
  * @param columnDefInt typed column definition
  * @tparam T type of the values hold by this column
  */
private[definition] class TypedColumnStore[T](columnDefInt: ColumnDef[T]) extends ColumnStore {
  override type valueType = T
  override def columnDef: UntypedColumnDef = columnDefInt
  private val data: mutable.Buffer[T] = mutable.Buffer.empty

  override def append(value: T): Unit = data.append(value)

  override def apply(idx: Int): T = data(idx)

  override def get(idx: Int): Option[T] =
    if(idx < data.length && idx >= 0)
      Some(data(idx))
    else
      None

  override def size: Int = data.size

  override def update(idx: Int, elem: T): Unit = data.update(idx, elem)

  override def indexOf[B >: T](elem: B): Int = data.indexOf(elem)

  override def indicesWhere(p: T => Boolean): Seq[Int] =
    data.zipWithIndex.collect{ case (value, index) if p(value) => index }

  override def isEmpty: Boolean = data.isEmpty

  override def contains[A1 >: T](elem: A1): Boolean = data.contains(elem)

  override def indices: Range = data.indices

  override def toString: String = s"TypedColumn[${columnDef.tpe}]($data)"
}
