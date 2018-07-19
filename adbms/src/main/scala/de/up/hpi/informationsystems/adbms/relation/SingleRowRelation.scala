package de.up.hpi.informationsystems.adbms.relation

import de.up.hpi.informationsystems.adbms.RecordNotFoundException
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef, UntypedColumnDef}
import de.up.hpi.informationsystems.adbms.record.Record

import scala.util.Try

object SingleRowRelation {

  /**
    * Creates an instance of a single row relation, which actually stores data.
    * Use a [[de.up.hpi.informationsystems.adbms.definition.RelationDef]] to define your relational schema.
    * Use like the following:
    *
    * @example {{{
    *           object MyRelation extends RelationDef {
    *             ...
    *           }
    *           val myRelation: MutableRelation = SingleRowRelation(MyRelation)
    * }}}
    *
    * @see [[de.up.hpi.informationsystems.adbms.definition.RelationDef]]
    */
  def apply(relDef: RelationDef): MutableRelation = new SingleRowRelation(relDef.columns)
}

class SingleRowRelation(pColumns: Set[UntypedColumnDef]) extends MutableRelation {
  
  private val cols: Vector[UntypedColumnDef] = pColumns.toVector
  private var data: Vector[Any] = Vector.empty

  /** @inheritdoc */
  override val columns: Set[UntypedColumnDef] = pColumns

  // from MutableRelation
  /** @inheritdoc */
  override def insert(record: Record): Try[Record] = Try{
    exceptionWhenNotEqual(record.columns)
    exceptionWhenAlreadyFull("insert")
    data = cols.map( col => record(col) )
    record
  }

  /** @inheritdoc */
  override def delete(record: Record): Try[Record] = Try{
    exceptionWhenNotEqual(record.columns)
    val tuple = cols.map( col => record(col) )
    if(data != tuple)
      throw RecordNotFoundException(s"this relation does not contain $record")
    data = Vector.empty
    record
  }

  /** @inheritdoc */
  override protected def internalUpdateByWhere(updateData: Map[UntypedColumnDef, Any], fs: Map[UntypedColumnDef, Any => Boolean]): Try[Int] = Try {
    exceptionWhenNotSubset(updateData.keys)
    val allFiltersApply = fs.keys
      .map { col: UntypedColumnDef => fs(col)(data(cols.indexOf(col))) }
      .forall(_ == true)

    if(allFiltersApply){
      data = updateData.keys.foldLeft(data)((t, updateCol) => t.updated(cols.indexOf(updateCol), updateData(updateCol)))
      1
    } else {
      0
    }
  }

  /** @inheritdoc */
  override def immutable: Relation = Relation(Seq(Record.fromVector(cols)(data)))

  // from Relation

  /** @inheritdoc */
  override def where[T](f: (ColumnDef[T], T => Boolean)): Relation = {
    val (columnDef, condition) = f
    val index = cols.indexOf(columnDef) // -1 --> IndexOutOfBoundsException

    if(condition(data(index).asInstanceOf[T]))
      this.immutable
    else
      Relation.empty
  }

  /** @inheritdoc */
  override def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): Relation = {
    val condResults = fs.keys.map( col => {
      val index = cols.indexOf(col)
      fs(col)(data(index))
    })
    if(condResults.forall(_ == true))
      this.immutable
    else
      Relation.empty
  }

  /** @inheritdoc */
  override def project(columnDefs: Set[UntypedColumnDef]): Relation = Relation(Try{
    exceptionWhenNotSubset(columnDefs)
    val newCols = columnDefs.toVector
    val newTuple = newCols.map( colDef => data(cols.indexOf(colDef)))
    Seq(Record.fromVector(newCols)(newTuple))
  })

  /** @inheritdoc */
  override def applyOn[T](col: ColumnDef[T], f: T => T): Relation = Relation(Try{
    exceptionWhenNotSubset(Seq(col))
    val index = cols.indexOf(col)
    val newValue = f(data(index).asInstanceOf[T])
    Seq(Record.fromVector(cols)(data.updated(index, newValue)))
  })

  /** @inheritdoc */
  override def records: Try[Seq[Record]] = Try(Seq(Record.fromVector(cols)(data)))

  @throws[UnsupportedOperationException]
  private def exceptionWhenAlreadyFull(op: String): Unit =
    if(data.length == 1)
      throw new UnsupportedOperationException(s"A single row relation can only contain one row! $op is not allowed anymore.")
}
