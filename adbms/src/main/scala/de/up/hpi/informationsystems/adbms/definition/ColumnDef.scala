package de.up.hpi.informationsystems.adbms
package definition

import scala.reflect.ClassTag

object ColumnDef {
  def apply[T](name: String)(implicit ct: ClassTag[T]): TypedColumnDef[T] = new TypedColumnDef[T](name)(ct)
}

/**
  * Column definition consisting of name and type information.
  * Can be used to define a relational schema for storing data.
  *
  * @see [[de.up.hpi.informationsystems.adbms.definition.ColumnRelation]]
  */
sealed trait ColumnDef {
  type value

  /**
    * Returns name of the column
    * @return name of the column
    */
  def name: String

  /**
    * Returns type of the column as a [[scala.reflect.ClassTag]]
    * @return type of the column as [[scala.reflect.ClassTag]]
    */
  def tpe: ClassTag[value]

  /**
    * Creates the corresponding [[de.up.hpi.informationsystems.adbms.definition.ColumnStore]]
    *
    * @return corresponding Column
    */
  protected[definition] def build(): ColumnStore
}

case class TypedColumnDef[T](name: String)(implicit ct: ClassTag[T]) extends ColumnDef {
  override type value = T
  override def tpe: ClassTag[T] = ct
  override protected[definition] def build(): TypedColumnStore[T] = ColumnStore[T](this)

  override def toString: String = s"""TypedColumnDef[$tpe](name="$name")"""
}