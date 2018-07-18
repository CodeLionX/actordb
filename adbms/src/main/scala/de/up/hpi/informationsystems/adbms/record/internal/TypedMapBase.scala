package de.up.hpi.informationsystems.adbms.record.internal

import scala.language.higherKinds

/**
  * A generic trait for an immutable map, which holds a mapping from
  * a type-annotated key and the corresponding value of its type. Use
  * [[de.up.hpi.informationsystems.adbms.record.internal.TypedMapLike]]
  * to get map-like functionality for this base.
  *
  * {{{
  *   class TypedMap[K[+_ <: V], V](override protected val data: Map[K[V], V])
  *     extends TypedMapBase[K, V]
  *       with TypedMapLike[K, V, TypedMap[K, V]]
  *   {
  *     override def ctor(cells: Map[K[V], V]): TypedMap[K, V] = new TypedMap[K, V](cells)
  *   }
  * }}}
  *
  * @tparam K type of key
  * @tparam V type of value
  */
trait TypedMapBase[K[+_ <: V], V] {

  protected val data: Map[K[V], V]

  // from java.lang.Object
  override def toString: String = s"${this.getClass.getSimpleName}(${data.mkString(", ")})"

  override def hashCode(): Int = 11 + data.hashCode()

  def canEqual(o: Any): Boolean = o.isInstanceOf[TypedMapBase[K, V]]

  override def equals(o: Any): Boolean = o match {
    case that: TypedMapBase[K, V] => that.canEqual(this) && this.data.equals(that.data)
    case _ => false
  }
}