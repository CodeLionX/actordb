package de.up.hpi.informationsystems.adbms.relation

import de.up.hpi.informationsystems.adbms.RecordNotFoundException
import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef}
import de.up.hpi.informationsystems.adbms.record.Record

import scala.reflect.ClassTag
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
  
  private val cols: Array[UntypedColumnDef] = pColumns.toArray
  private var data: Array[Any] = Array.empty

  private implicit class RichDataArray(tuple: Array[Any]) {

    def toRecordSeq(columnDefs: Array[UntypedColumnDef]): Seq[Record] =
      if(tuple.isEmpty)
        Seq.empty
      else
        Seq(Record.fromArray(columnDefs)(tuple))

    def toRelation(columnDefs: Array[UntypedColumnDef]): Relation =
      if(tuple.isEmpty)
        Relation.empty
      else
        Relation(Seq(
          Record.fromArray(columnDefs)(tuple)
        ))
  }

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

  /** This operation is not supported for `SingleRowRelation`s.
    *
    * Batch insert is only plausible if there are more than one record inserted.
    * As a `SingleRowRelation` can only contain one record, `insert()` should be
    * used instead.
    *
    * @return `Try[Seq[Record] ]` containing an [[UnsupportedOperationException]]
    */
  override def insertAll(records: Seq[Record]): Try[Seq[Record]] = Try{
    throw new UnsupportedOperationException("A single row relation can only contain one row! Use `insert()` instead.")
  }

  /** @inheritdoc */
  override def delete(record: Record): Try[Record] = Try{
    exceptionWhenNotEqual(record.columns)
    val tuple = cols.map( col => record(col) )
    if(!data.sameElements(tuple))
      throw RecordNotFoundException(s"this relation does not contain $record")
    data = Array.empty
    record
  }

  /** @inheritdoc */
  override protected def internalUpdateByWhere(updateData: Map[UntypedColumnDef, Any], fs: Map[UntypedColumnDef, Any => Boolean]): Try[Int] = Try {
    exceptionWhenNotSubset(updateData.keys)
    if(data.nonEmpty) {
      val allFiltersApply = fs.keys
        .map { col: UntypedColumnDef => fs(col)(data(cols.indexOf(col))) }
        .forall(identity)

      if(allFiltersApply){
        data = updateData.keys.foldLeft(data)( (t, updateCol) =>
          t.updated(cols.indexOf(updateCol), updateData(updateCol))
        )
        1
      } else {
        0
      }
    } else
      0
  }

  /** @inheritdoc */
  override def immutable: Relation = data.toRelation(cols)

  // from Relation

  /** @inheritdoc */
  override def where[T : ClassTag](f: (ColumnDef[T], T => Boolean)): Relation = Relation(Try{
    val (columnDef, condition) = f
    exceptionWhenNotSubset(Set(columnDef))

    if (data.nonEmpty) {
      val index = cols.indexOf(columnDef)
      if (condition(data(index).asInstanceOf[T]))
        data.toRecordSeq(cols)
      else
        Seq.empty
    } else
      Seq.empty
  })

  /** @inheritdoc */
  override def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): Relation = Relation(Try{
    exceptionWhenNotSubset(fs.keySet)

    if(data.nonEmpty) {
      val condResults = fs.keys.map(col => {
        val index = cols.indexOf(col)
        fs(col)(data(index))
      })
      if (condResults.forall(identity))
        data.toRecordSeq(cols)
      else
        Seq.empty
    } else
      Seq.empty
  })

  /** @inheritdoc */
  override def project(columnDefs: Set[UntypedColumnDef]): Relation = Relation(Try{
    exceptionWhenNotSubset(columnDefs)

    if (data.nonEmpty) {
      val newCols = columnDefs.toArray
      val newTuple = newCols.map(colDef => data(cols.indexOf(colDef)))
      newTuple.toRecordSeq(newCols)
    } else
      Seq.empty
  })

  /** @inheritdoc */
  override def applyOn[T : ClassTag](col: ColumnDef[T], f: T => T): Relation = Relation(Try{
    exceptionWhenNotSubset(Seq(col))

    if (data.nonEmpty) {
      val index = cols.indexOf(col)
      val newValue = f(data(index).asInstanceOf[T])
      data.updated(index, newValue).toRecordSeq(cols)
    } else
      Seq.empty
  })

  /** @inheritdoc */
  override def records: Try[Seq[Record]] = Try(data.toRecordSeq(cols))

  @throws[UnsupportedOperationException]
  private def exceptionWhenAlreadyFull(op: String): Unit =
    if(data.nonEmpty)
      throw new UnsupportedOperationException(s"A single row relation can only contain one row! $op is not allowed anymore.")

}
