package de.up.hpi.informationsystems.adbms
package definition

import java.util.Objects

import scala.reflect.ClassTag
import scala.language.implicitConversions

object ColumnDef {

  @deprecated
  def apply[T](name: String)(implicit ct: ClassTag[T]): ColumnDef[T] = new ColumnDef[T](name, null.asInstanceOf[T])(ct)

  def apply[T](name: String, default: T)(implicit ct: ClassTag[T]): ColumnDef[T] = new ColumnDef[T](name, default)(ct)

  implicit def columnDefSet2UntypedSet[T](set: Set[ColumnDef[T]]): Set[UntypedColumnDef] =
    set.asInstanceOf[Set[UntypedColumnDef]]
}

/**
  * Column definition consisting of name and type information.
  * Can be used to define a relational schema for storing data.
  *
  * @see [[de.up.hpi.informationsystems.adbms.definition.ColumnRelation]]
  */
sealed trait UntypedColumnDef {
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

  def default: value

  /**
    * Returns an untyped version of this column definition
    * @return untyped version of this column definition
    */
  def untyped: UntypedColumnDef

  /**
    * Creates the corresponding [[de.up.hpi.informationsystems.adbms.definition.ColumnStore]]
    *
    * @return corresponding Column
    */
  protected[definition] def buildColumnStore(): ColumnStore
}

final class ColumnDef[T](pName: String, pDefault: T)(implicit ct: ClassTag[T]) extends UntypedColumnDef {

  override type value = T

  override val name: String = pName

  override val tpe: ClassTag[T] = ct

  override val default: T = pDefault

  override def untyped: UntypedColumnDef = this.asInstanceOf[UntypedColumnDef]

  override protected[definition] def buildColumnStore(): TypedColumnStore[T] = ColumnStore[T](this)

  // overrides of [[java.lang.Object]]

  override def toString: String = s"""${this.getClass.getSimpleName}[$tpe](name="$name")"""

  override def hashCode(): Int = Objects.hash(name, ct)

  override def equals(o: scala.Any): Boolean =
    if (o == null || getClass != o.getClass)
      false
    else {
      // cast other object
      val otherTypedColumnDef: ColumnDef[T] = o.asInstanceOf[ColumnDef[T]]
      if (this.name.equals(otherTypedColumnDef.name) && this.tpe.equals(otherTypedColumnDef.tpe))
        true
      else
        false
    }

  override def clone(): AnyRef = new ColumnDef[T](this.name, this.default)(this.tpe)
}