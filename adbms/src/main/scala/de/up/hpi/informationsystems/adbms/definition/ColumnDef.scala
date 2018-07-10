package de.up.hpi.informationsystems.adbms
package definition

import java.util.Objects

import scala.language.implicitConversions
import scala.reflect.ClassTag

object ColumnDef {

  def apply[T](name: String)(implicit default: ColumnTypeDefault[T]): ColumnDef[T] =
    new ColumnDef[T](name, default.default)(default.ct)

  def apply[T](name: String, default: T)(implicit ct: ClassTag[T]): ColumnDef[T] =
    new ColumnDef[T](name, default)(ct)

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

  /**
    * Returns the default value of the column.
    * @return the default value of the column
    */
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

  override def toString: String = s"""${this.getClass.getSimpleName}[$tpe](name="$name", default=$default)"""

  // proper way to implement equals() in scala:
  // https://www.safaribooksonline.com/library/view/scala-cookbook/9781449340292/ch04s16.html
  // -----
  override def hashCode(): Int = Objects.hash(name, tpe) + (if(default == null) 0 else default.hashCode())

  def canEqual(o: Any): Boolean = o.isInstanceOf[ColumnDef[T]]

  override def equals(o: Any): Boolean = o match {
    case o: ColumnDef[T] => o.canEqual(this) && this.hashCode() == o.hashCode()
    case _ => false
  }
  // -----
}