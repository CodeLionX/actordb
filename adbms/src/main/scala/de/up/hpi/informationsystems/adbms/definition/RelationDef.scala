package de.up.hpi.informationsystems.adbms.definition

import de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilder

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