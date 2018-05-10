package de.up.hpi.informationsystems.adbms.definition
import scala.util.Try

/**
  * Defines a column-oriented relation schema, which's data store gets automatically generated.
  *
  * @deprecated in favor of RowRelation since 09/05/2018
  */
@deprecated("Was deprecated in favor of RowRelation", "09/05/2018")
abstract class ColumnRelation extends Relation {

  // needs to be lazy evaluated, because `columns` is not yet defined when this class gets instantiated
  private lazy val data: Map[UntypedColumnDef, ColumnStore] =
    columns.map { colDef: UntypedColumnDef =>
      Map(colDef -> colDef.buildColumnStore())
    }.reduce(_ ++ _)

  private def getRecord(selectedColumns: Seq[UntypedColumnDef])(idx: Int): Record = {
    selectedColumns
      .foldLeft( Record(selectedColumns) )( (builder, column) => {
        val columnStore = data(column) // needed to get the right type here 🡫
        builder.withCellContent(column.asInstanceOf[ColumnDef[columnStore.valueType]])(columnStore(idx))
      })
      .build()
  }

  /** @inheritdoc */
  override def insert(record: Record): Unit = {
    columns.foreach(column => {
      val columnStore = data(column)
      columnStore.append(record(column).asInstanceOf[columnStore.valueType])
    })
  }

  /** @inheritdoc */
  override def where[T](f: (ColumnDef[T], T => Boolean)): Seq[Record] = {
    val columnStore = data(f._1.untyped) // needed to get the right type 2 lines below
    columnStore
      .indicesWhere(f._2.asInstanceOf[columnStore.valueType => Boolean])
      .map(getRecord(columns)(_))
  }

  /** @inheritdoc */
  override def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): Seq[Record] =
    fs.keys
      .map(column =>
        data(column)
          .indicesWhere(fs(column))
          .map(getRecord(columns)(_))
          .toSet
      ).reduce( (a1, a2) => a1.intersect(a2) )
      .toSeq

  /** @inheritdoc */
  override def project(columnDefs: Seq[UntypedColumnDef]): Try[Seq[Record]] = Try(
    if(columnDefs.toSet subsetOf columns.toSet)
      (0 until data.size).map(getRecord(columnDefs)(_))
    else
      throw IncompatibleColumnDefinitionException(s"this relation does not contain all specified columns {$columnDefs}")
  )

  override def toString: String = {
    val header = columns.map { c => s"${c.name}[${c.tpe}]" }.mkString(" | ")
    val line = "-" * header.length
    var content: String = ""
    for (i <- 0 to data.size) {
      val col: Seq[ColumnStore] = columns.map(data)
      content = content + col.map(_.get(i)).mkString(" | ") + "\n"
    }
    header + "\n" + line + "\n" + content + "\n" + line + "\n"
  }
}