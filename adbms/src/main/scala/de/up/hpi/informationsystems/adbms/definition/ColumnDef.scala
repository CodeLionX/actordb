package de.up.hpi.informationsystems.adbms
package definition

import java.util.Objects

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
  /**
    * Holds the type of the contained values.
    */
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
  protected[definition] def buildColumnStore(): ColumnStore
}

final class TypedColumnDef[T](pName: String)(implicit ct: ClassTag[T]) extends ColumnDef {
  override type value = T

  override val name: String = pName

  override val tpe: ClassTag[T] = ct

  override protected[definition] def buildColumnStore(): TypedColumnStore[T] = ColumnStore[T](this)

  // overrides of [[java.lang.Object]]

  override def toString: String = s"""TypedColumnDef[$tpe](name="$name")"""

  override def hashCode(): Int = Objects.hash(name, ct)

  override def equals(o: scala.Any): Boolean =
    if (o == null || getClass != o.getClass)
      false
    else {
      // cast other object
      val otherTypedColumnDef: TypedColumnDef[T] = o.asInstanceOf[TypedColumnDef[T]]
      if (this.name.equals(otherTypedColumnDef.name) && this.tpe.equals(otherTypedColumnDef.tpe))
        true
      else
        false
    }

  override def clone(): AnyRef = new TypedColumnDef[T](this.name)(this.tpe)
}