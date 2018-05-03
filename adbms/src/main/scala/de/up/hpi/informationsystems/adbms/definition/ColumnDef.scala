package de.up.hpi.informationsystems.adbms
package definition

import scala.reflect.ClassTag

object ColumnDef {
  def apply[T](name: String)(implicit ct: ClassTag[T]): TypedColumnDef[T] = new TypedColumnDef[T](name)(ct)
}

sealed trait ColumnDef {
  type value
  def name: String
  def tpe: ClassTag[value]
  protected[definition] def build(): Column
}

case class TypedColumnDef[T](name: String)(implicit ct: ClassTag[T]) extends ColumnDef {
  override type value = T
  override def tpe: ClassTag[T] = ct
  override protected[definition] def build(): TypedColumn[T] = Column[T](this)

  override def toString: String = s"""TypedColumnDef[$tpe](name="$name")"""
}