package de.up.hpi.informationsystems.adbms.definition

import scala.reflect.ClassTag

class ColumnTypeDefault[A](val default: A)(implicit val ct: ClassTag[A])

trait LowerPriorityImplicits {
  // only regard AnyRefs
  implicit def defaultNull[A <: AnyRef](implicit classTag: ClassTag[A]): ColumnTypeDefault[A] = new ColumnTypeDefault[A](null.asInstanceOf[A])(classTag)
}

/**
  * Provides default values for column definition.
  *
  * Thanks to <i>iain</i> for
  * <a href="https://stackoverflow.com/questions/5260298/how-can-i-obtain-the-default-value-for-a-type-in-scala">
  * his post at stackoverflow
  * </a>.
  *
  * @see [[de.up.hpi.informationsystems.adbms.definition.ColumnDef]]
  */
object ColumnTypeDefaults extends LowerPriorityImplicits {

  implicit object DefaultDouble extends ColumnTypeDefault[Double](0.0)
  implicit object DefaultFloat extends ColumnTypeDefault[Float](0.0F)
  implicit object DefaultInt extends ColumnTypeDefault[Int](0)
  implicit object DefaultLong extends ColumnTypeDefault[Long](0L)
  implicit object DefaultShort extends ColumnTypeDefault[Short](0)
  implicit object DefaultByte extends ColumnTypeDefault[Byte](0)
  implicit object DefaultChar extends ColumnTypeDefault[Char]('\u0000')
  implicit object DefaultBoolean extends ColumnTypeDefault[Boolean](false)
  implicit object DefaultUnit extends ColumnTypeDefault[Unit](())

  // override string
  implicit val emptyStringAsDefault: ColumnTypeDefault[String] = new ColumnTypeDefault[String]("")

  implicit def defaultSeq[A]: ColumnTypeDefault[Seq[A]] = new ColumnTypeDefault[Seq[A]](Seq.empty[A])
  implicit def defaultSet[A]: ColumnTypeDefault[Set[A]] = new ColumnTypeDefault[Set[A]](Set.empty[A])
  implicit def defaultMap[A, B]: ColumnTypeDefault[Map[A, B]] = new ColumnTypeDefault[Map[A, B]](Map.empty[A, B])
  implicit def defaultOption[A]: ColumnTypeDefault[Option[A]] = new ColumnTypeDefault[Option[A]](None)

  /**
    * Return implicitly derived default value for type `A`.
    * @param value implicit default value
    * @tparam A type
    * @return default value for type `A`
    */
  def value[A](implicit value: ColumnTypeDefault[A]): A = value.default

}
