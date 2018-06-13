package de.up.hpi.informationsystems.adbms.definition

import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.record.Record.RecordBuilder

/**
  * Can be used for defining relations.
  *
  * @example {{{
  *           object TestRelation extends RelationDef {
  *             override val name: String = "testrelation"
  *
  *             val col1: ColumnDef[Int] = ColumnDef("col1")
  *             val col2: ColumnDef[String] = ColumnDef("col2")
  *
  *             override val columns: Set[UntypedColumnDef] = Set(col1, col2)
  *
  *           }
  *
  *           // `RowRelation` can be replaced by any other relational store
  *           val testRelation: MutableRelation = RowRelation(TestRelation)
  *           testRelation.insert(
  *             TestRelation.newRecord.build()
  *           )
  *           val records: Relation = testRelation.where(TestRelation.col1 -> { _ == 1 })
  *          }}}
  */
trait RelationDef {

  /**
    * Name that should be used for this relation.
    */
  val name: String

  /**
    * Returns the column definitions of this relation.
    * @note override this value to define your relational schema
    * @return a sequence of column definitions
    */
  val columns: Set[UntypedColumnDef]

  /**
    * Returns a new [[de.up.hpi.informationsystems.adbms.record.Record.RecordBuilder]] initialized with the
    * columns for this relation.
    * @return initialized RecordBuilder
    */
  def newRecord: RecordBuilder = Record(columns)

}
