package de.up.hpi.informationsystems.adbms.record.internal

import scala.language.higherKinds

/**
  * Ready-to-use typed map, which associate typed keys with their corresponding values.
  * Each key/value binding can have a different type bound by `V`.
  *
  * @param cells initial data
  * @tparam K type of key
  * @tparam V type of value
  */
class TypedMap[K[+_ <: V], V] private (cells: Map[K[V], V])
  extends TypedMapBase[K, V]
    with TypedMapLike[K, V, TypedMap[K, V]] {

  override protected val data: Map[K[V], V] = cells

  override protected def ctor(cells: Map[K[V], V]): TypedMap[K, V] = new TypedMap[K, V](cells)
}

object TypedMap{

  def apply[K[+_ <: V], V](data: Map[K[V], V]): TypedMap[K, V] = new TypedMap[K, V](data)

  def empty[K[+_ <: V], V]: TypedMap[K, V] = new TypedMap[K, V](Map.empty)

}