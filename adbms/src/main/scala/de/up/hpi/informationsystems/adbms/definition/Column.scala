package de.up.hpi.informationsystems.adbms
package definition

private[definition] object Column {
  def apply[T](columnDef: TypedColumnDef[T]): TypedColumn[T] = new TypedColumn[T](columnDef)
}

private[definition] sealed trait Column {
  type valueType
  def columnDef: ColumnDef
  def append(value: valueType): Unit
  def get(idx: Int): Option[valueType]
  def length: Int
}

private[definition] class TypedColumn[T](columnDefInt: TypedColumnDef[T]) extends Column {
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
