package de.up.hpi.informationsystems.adbms
package definition

import java.util.Objects

import scala.language.implicitConversions
import scala.reflect.ClassTag

object ColumnDef {

  type UntypedColumnDef = ColumnDef[Any]

  def apply[T](name: String)(implicit default: ColumnTypeDefault[T]): ColumnDef[T] =
    new ColumnDef[T](name, default.default)(default.ct)

  def apply[T](name: String, default: T)(implicit ct: ClassTag[T]): ColumnDef[T] =
    new ColumnDef[T](name, default)(ct)

}

/**
  * Column definition consisting of name, type information and a default value.
  * Can be used to define a relational schema for storing data.
  */
class ColumnDef[+T <: Any](pName: String, pDefault: T)(implicit ct: ClassTag[T]) {

//  override type value = T

  /**
    * Returns name of the column
    * @return name of the column
    */
  val name: String = pName

  /**
    * Returns type of the column as a [[scala.reflect.ClassTag]]
    * @return type of the column as [[scala.reflect.ClassTag]]
    */
  val tpe: ClassTag[_] = ct

  /**
    * Returns the default value of the column.
    * @return the default value of the column
    */
  val default: T = pDefault

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