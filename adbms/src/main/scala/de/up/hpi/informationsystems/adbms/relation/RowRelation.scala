package de.up.hpi.informationsystems.adbms.relation

import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.{IncompatibleColumnDefinitionException, RecordNotFoundException, Util}

import scala.reflect.ClassTag
import scala.util.Try

object RowRelation {

  /**
    * Creates an instance of a row relation, which actually stores data.
    * Use a [[de.up.hpi.informationsystems.adbms.definition.RelationDef]] to define your relational schema.
    * Use like the following:
    *
    * @example {{{
    *           object MyRelation extends RelationDef {
    *             ...
    *           }
    *           val myRelation: MutableRelation = RowRelation(MyRelation)
    * }}}
    *
    * @see [[de.up.hpi.informationsystems.adbms.definition.RelationDef]]
    */
  def apply(relDef: RelationDef): MutableRelation = new RowRelation(relDef.columns)
}

private final class RowRelation(passedColumns: Set[UntypedColumnDef]) extends MutableRelation {

  private val cols: Vector[UntypedColumnDef] = passedColumns.toVector
  private var data: Vector[Vector[Any]] = Vector.empty

  private implicit class RichDataVector(in: Vector[Vector[Any]]) {
    /**
      * Convertes this `Vector[ Vector[Any] ]` into a sequence of records `Seq[Record]`.
      * This is the preferred data type meant for the user of this framework.
      * @param columns used to match the column definition with the cell contents
      * @return a sequence of records containing the data of this array
      */
    def toRecordSeq(columns: Vector[UntypedColumnDef]): Seq[Record] =
      in.map(Record.fromVector(columns))
  }

  /** @inheritdoc */
  override val columns: Set[UntypedColumnDef] = passedColumns

  /** @inheritdoc */
  override def insert(record: Record): Try[Record] = Try{
    exceptionWhenNotEqual(record.columns)
    data = data :+ cols.map( col => record(col) ) // ensure ordering is the same than `cols`
    record
  }

  /** @inheritdoc*/
  override def delete(record: Record): Try[Record] = Try{
    exceptionWhenNotEqual(record.columns)
    val tuple = cols.map( col => record(col) )
    if(!data.contains(tuple))
      throw RecordNotFoundException(s"this relation does not contain the record: $record")
    data = data.filterNot(_ == tuple)
    record
  }

  /** @inheritdoc*/
  override protected def internalUpdateByWhere(
        updateData: Map[UntypedColumnDef, Any], fs: Map[UntypedColumnDef, Any => Boolean]
      ): Try[Int] = Try {
    exceptionWhenNotSubset(updateData.keys)
    var counter = 0
    data = data.map( tuple => {
      val allFiltersApply = fs.keys
        .map { col: UntypedColumnDef => fs(col)(tuple(cols.indexOf(col))) }
        .forall(_ == true)

      if(allFiltersApply){
        counter += 1
        updateData.keys.foldLeft(tuple)((t, updateCol) => t.updated(cols.indexOf(updateCol), updateData(updateCol)))
      }
      else
        tuple
    })
    counter
  }

  /** @inheritdoc */
  override def where[T : ClassTag](f: (ColumnDef[T], T => Boolean)): Relation = {
    val (columnDef, condition) = f
    val index = cols.indexOf(columnDef)
    Relation(
      data.filter{ tuple =>
        condition(tuple(index).asInstanceOf[T])
      }.toRecordSeq(cols)
    )
  }

  /** @inheritdoc */
  override def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): Relation = {
    Relation(
      data.filter{ tuple =>
        fs.keys.map( col => {
          val index = cols.indexOf(col)
          fs(col)(tuple(index))
        }).forall(_ == true)
      }.toRecordSeq(cols)
    )
  }

  /** @inheritdoc */
  override def project(columnDefs: Set[UntypedColumnDef]): Relation =
    Relation(Try {
      exceptionWhenNotSubset(columnDefs)
      val newCols = columnDefs.toVector

      data.map(tuple =>
        newCols.map(colDef => tuple(cols.indexOf(colDef)))
      ).toRecordSeq(newCols)
    })

  /** @inheritdoc */
  override def applyOn[T : ClassTag](col: ColumnDef[T], f: T => T): Relation =
      if(!columns.contains(col))
        this.immutable
      else
        Relation(Try{
          val index = cols.indexOf(col)
          data.map( tuple => {
            val newValue = f(tuple(index).asInstanceOf[T])
            tuple.updated(index, newValue)
          }).toRecordSeq(cols)
        })

  /** @inheritdoc */
  override def records: Try[Seq[Record]] = Try(data.toRecordSeq(cols))

  /** @inheritdoc */
  override def toString: String = s"${this.getClass.getSimpleName}:\n" + Util.prettyTable(columns, data.toRecordSeq(cols))

  /** @inheritdoc*/
  override def immutable: Relation = Relation(data.toRecordSeq(cols))

  @throws[IncompatibleColumnDefinitionException]
  private def exceptionWhenNotSubset(incomingColumns: Iterable[UntypedColumnDef]): Unit =
    if (!(incomingColumns.toSet subsetOf columns)) {
      val notMatchingColumns = incomingColumns.toSet -- columns
      throw IncompatibleColumnDefinitionException(s"this relation does not contain following columns: $notMatchingColumns")
    }

  @throws[IncompatibleColumnDefinitionException]
  private def exceptionWhenNotEqual(incomingColumns: Iterable[UntypedColumnDef]): Unit =
    if(incomingColumns != columns)
      throw IncompatibleColumnDefinitionException(s"the provided column layout does not match this " +
        s"relation's schema:\n$incomingColumns (provided)\n${this.columns} (relation)")

}