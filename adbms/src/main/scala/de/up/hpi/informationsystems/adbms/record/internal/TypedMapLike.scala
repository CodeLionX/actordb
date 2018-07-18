package de.up.hpi.informationsystems.adbms.record.internal

import java.util.NoSuchElementException

import scala.language.higherKinds
import scala.reflect.ClassTag


/**
  * A template trait for maps, which associate typed keys with their corresponding values.
  * Each key/value binding can have a different type bound by `V`.
  * This trait provides most of the operations of a `TypedMap` independently of its representation.
  * It is typically inherited by concrete implementations of maps.
  *
  * @tparam K    the type of the keys
  * @tparam V    the type of the associated values
  * @tparam This the type of the map itself
  */
trait TypedMapLike[K[+_ <: V], V, This <: TypedMapBase[K, V]] { self: This =>

  /** Creates a new instance of `This` using the provided data.
    * @param cells content of the new `This` instance
    * @return a new `This` using the new content
    */
  protected def ctor(cells: Map[K[V], V]): This

  /** Creates a new iterator over all key/value pairs of this map
    *
    *  @return the new iterator
    */
  def iterator: Iterator[(K[V], V)]

  /** Adds a key/value pair to this map, returning a new map.
    *  @param    kv the key/value pair
    *  @tparam   T the type of the value in the key/value pair.
    *  @return   a new `This` with the new binding added to it
    */
  def +[T <: V](kv: (K[T], T)): This =
    ctor(data + kv)

  /** Removes a key from this map, returning a new map.
    *  @param    k the key to be removed
    *  @return   a new `This` without a binding for `key`
    */
  def -[T <: V](k: K[T]): This =
    ctor(data - k)

  /** Adds a number of elements provided by a traversable object
    *  and returns a new collection with the added elements.
    *
    *  @param xs      the traversable object consisting of key-value pairs.
    *  @return        a new `This` with the bindings of this map and those from `xs`.
    */
  def ++[T <: V](xs: TypedMapBase[K, T]): This =
    ctor(data.++(xs.iterator))

  /** Creates a new map from this one by removing all keys of another
    *  collection.
    *
    *  @param xs     the collection containing the removed elements.
    *  @return a new `This` that contains all key/value bindings of the current map
    *  except the key/value bindings of `elems`.
    */
  def --(xs: TypedMapBase[K, V]): This =
    ctor(data.--(xs.iterator.map(_._1)))

  /** Retrieves the value which is associated with the given key. This
    *  method invokes the `default` method of the map if there is no mapping
    *  from the given key to a value. Unless overridden, the `default` method throws a
    *  `NoSuchElementException`.
    *
    *  @param  k   the key
    *  @tparam T   the type of the value in the key/value binding
    *  @return     the value associated with the given key, or the result of the
    *              map's `default` method, if none exists.
    */
  def apply[T <: V : ClassTag](k: K[T]): T =
    data(k) match {
      case t: T => t
      case _ => default(k)
    }

  /** Optionally returns the value associated with a key.
    *
    *  @param  k    the key value
    *  @tparam T    the type of the value in the key/value binding
    *  @return an option value containing the value associated with `key` in this map,
    *          or `None` if none exists.
    */
  def get[T <: V : ClassTag](k: K[T]): Option[T] =
    data.get(k).flatMap{
      case t: T => Some(t)
      case _ => None
    }

  /** A new immutable map containing updating this map with a given key/value mapping.
    *  @param    key the key
    *  @param    value the value
    *  @return   A new map with the new key/value mapping
    */
  def updated[T <: V](key: K[T], value: T): This = this + ((key, value))

  /** Defines the default value computation for the map,
    *  returned when a key is not found
    *  The method implemented here throws an exception,
    *  but it might be overridden in subclasses.
    *
    *  @param key the given key value for which a binding is missing.
    *  @tparam T  the type of the value in the key/value binding
    *  @throws NoSuchElementException if the key was not found
    */
  def default[T <: V](key: K[T]): T =
    throw new NoSuchElementException("key not found: " + key)

  /** Converts this map to a sequence. As with `toIterable`, it's lazy
    *  in this default implementation, as this `TypedMapLike` may be
    *  lazy and unevaluated.
    *
    *  @return a sequence containing all elements of this map as key/value-tuples.
    */
  def toSeq: Seq[(K[V], V)] = data.toSeq

  /** The empty map of the same type as this map
    *   @return   an empty map of type `This`.
    */
  def empty: This = ctor(Map.empty)

}