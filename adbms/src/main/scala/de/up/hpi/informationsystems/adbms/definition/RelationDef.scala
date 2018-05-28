package de.up.hpi.informationsystems.adbms.definition

import de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilder

/**
  * Can be used for defining relations.
  *
  * @example {{{
  *           object TestRelationDef extends RelationDef {
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
  *           val testRelStore: MutableRelation = RowRelation(TestRelationDef)
  *           testRelStore.insert(
  *             TestRelationDef.newRecord.build()
  *           )
  *           val records: Relation = testRelStore.where(TestRelationDef.col1 -> { _ == 1 })
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
    * Returns a new [[de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilder]] initialized with the
    * columns for this relation.
    * @return initialized RecordBuilder
    */
  def newRecord: RecordBuilder = Record(columns)

}