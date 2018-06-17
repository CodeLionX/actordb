package de.up.hpi.informationsystems.adbms.record

import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, UntypedColumnDef}

/**
  * Provides implicits for creating a mapping between column definition and cell value
  *
  * @example {{{
  * import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
  * val x: ColumnCellMapping = ColumnDef[Int]("id") ~> 12
  * }}}
  */
object ColumnCellMapping {
  implicit class ColumnCellMapper[T](in: ColumnDef[T]) {

    /**
      * Syntax-sugar for creating a mapping of column definition and cell value for the use in e.g.
      * [[de.up.hpi.informationsystems.adbms.record.Record.RecordBuilder]]. Returns a
      * [[de.up.hpi.informationsystems.adbms.record.ColumnCellMapping]].
      *
      * @param value cell value
      * @return a new [[de.up.hpi.informationsystems.adbms.record.ColumnCellMapping]] containing
      *         the column definition and cell value mapping
      */
    def ~>(value: T): ColumnCellMapping = new ColumnCellMapping(Map(in.untyped -> value))
  }
}

final class ColumnCellMapping private[adbms](private val columnCellMapping: Map[UntypedColumnDef, Any]){
  def +(other: ColumnCellMapping): ColumnCellMapping = and(other)
  def &(other: ColumnCellMapping): ColumnCellMapping = and(other)
  def and(other: ColumnCellMapping): ColumnCellMapping =
    new ColumnCellMapping(this.columnCellMapping ++ other.toMap)

  private[adbms] def toMap: Map[UntypedColumnDef, Any] = columnCellMapping
}

