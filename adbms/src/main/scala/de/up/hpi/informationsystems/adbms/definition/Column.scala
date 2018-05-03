package de.up.hpi.informationsystems.adbms
package definition

private[definition] object Column {
  def apply[T](columnDef: TypedColumnDef[T]): TypedColumn[T] = new TypedColumn[T](columnDef)
}

private[definition] sealed trait Column {
  type valueType
  def columnDef: ColumnDef
  def append(value: valueType): Unit
  def get(idx: Int): Option[valueType]
  def length: Int
}

private[definition] class TypedColumn[T](columnDefInt: TypedColumnDef[T]) extends Column {
  override type valueType = T
  override def columnDef: ColumnDef = columnDefInt
  private var data: Seq[T] = Seq.empty

  override def append(value: T): Unit =
    data = data :+ value

  override def get(idx: Int): Option[T] =
    if(idx < data.length && idx >= 0)
      Some(data(idx))
    else
      None

  override def length: Int = data.length

  override def toString: String = s"TypedColumn[${columnDef.tpe}]($data)"
}

/*
import scala.collection.immutable.Seq
import scala.collection.mutable.{Seq => MSeq}
import scala.reflect.ClassTag

object ColumnDef {
  def of[T <: Any](name: String)(implicit ev: ClassTag[T]) = new ColumnDef[T](name)
}

final class ColumnDef[+T <: Any](name: String)(implicit ev: ClassTag[T]) {
  //val valueType: ClassTag[T] = ev
  override def toString: String = s"ColumnDef($name)"
}

object RowRelationDef {
  def apply(columnDefs: Seq[ColumnDef[Any]]): RowRelationDef = new RowRelationDef(columnDefs)

}
final class RowRelationDef(val columns: Seq[ColumnDef[_ <: Any]]) {
  def apply(): RowRelation = new RowRelation(this)
  def createRecord[Seq[ColumnDef[T]]](values: Seq[Any]) =
    new RecordImpl(columns.zip(values).map{ case (column: ColumnDef[Any], value: Any) => Map(column -> value)}.reduce(_ ++ _))
    //new RecordImpl()
}


sealed trait Record {
  def apply[T <: Any](columnDef: ColumnDef[T]): T
  def get[T <: Any](columnDef: ColumnDef[T]): T
}
class RecordImpl(private val cells: Map[ColumnDef[_ <: Any], Any]) extends Record {

  def apply[T <: Any](columnDef: ColumnDef[T]): T = cells.apply(columnDef).asInstanceOf[T]

  def get[T <: Any](columnDef: ColumnDef[T]): T = cells.get(columnDef).asInstanceOf[T]

  override def toString: String = s"Record($cells)"
}

class RowRelation(private var definition: RowRelationDef) {
  val columns: Seq[ColumnDef] = definition.columns
  private var store: MSeq[Record] = MSeq.empty

  def insert(record: Record): Unit =
    store = store :+ record

  def last: Record =
    store.last

  override def toString: String = store.toString
}

object Main extends App {
  val relDef = RowRelationDef(Seq(
    ColumnDef.of[String]("Lastname"),
    ColumnDef.of[Int]("Age")
  ))

  println(relDef.columns)
  val rel = relDef()
}
*/