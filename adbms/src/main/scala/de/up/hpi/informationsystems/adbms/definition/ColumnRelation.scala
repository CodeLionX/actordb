package de.up.hpi.informationsystems.adbms.definition


sealed trait ColumnRelation extends Relation

/**
  * Defines a column-oriented relation schema, which's store gets automatically generated.
  */
object ColumnRelation {

  /**
    * Defines a column-oriented relation schema, which gets automatically generated.
    *
    * @param columnDefs sequence of column definitions
    * @return the generated column-oriented relational store
    */
  def apply(columnDefs: Seq[UntypedColumnDef]): ColumnRelation = new ColumnRelationStore(columnDefs)

  /**
    * Indicates that a [[de.up.hpi.informationsystems.adbms.definition.UntypedColumnDef]] was not found in
    * the column relation.
    *
    * @param message gives details
    */
  class ColumnNotFoundException(message: String) extends Exception(message) {
    def this(message: String, cause: Throwable) = {
      this(message)
      initCause(cause)
    }

    def this(cause: Throwable) = this(cause.toString, cause)

    def this() = this(null: String)
  }

  /**
    * Private (hidden) implementation of the [[de.up.hpi.informationsystems.adbms.definition.ColumnRelation]] trait.
    * @param colDefs column definitions used to construct the underlying data store
    */
  private final class ColumnRelationStore(private val colDefs: Seq[UntypedColumnDef]) extends ColumnRelation {

    private val data: Map[UntypedColumnDef, ColumnStore] =
      colDefs.map { colDef: UntypedColumnDef =>
        Map(colDef -> colDef.buildColumnStore())
      }.reduce(_ ++ _)

    private var n: Int = 0


    /** @inheritdoc */
    override def columns: Seq[UntypedColumnDef] = colDefs

    /** @inheritdoc */
    override def insert(record: Record): Unit = {
      n += 1
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
        .map(idx =>
          columns.foldLeft(Record(columns))((builder, column) =>
            builder.withCellContent(column.asInstanceOf[ColumnDef[T]] -> data(column)(idx).asInstanceOf[T])
          ).build()
        )
    }

    /** @inheritdoc */
    override def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): Seq[Record] =
      fs.keys.map(column =>
          data(column).indicesWhere(fs(column)).map(idx =>
            columns.foldLeft(Record(columns))((builder, column) => {
              val columnStore = data(column) // needed to get the right type here 🡫
              builder.withCellContent(column.asInstanceOf[ColumnDef[columnStore.valueType]] -> columnStore(idx))
            }).build()
          ).toSet
        ).reduce( (a1, a2) => a1.intersect(a2) )
        .toSeq

    override def toString: String = {
      val header = columns.map { c => s"${c.name}[${c.tpe}]" }.mkString(" | ")
      val line = "-" * header.length
      var content: String = ""
      for (i <- 0 to n) {
        val col: Seq[ColumnStore] = columns.map(data)
        content = content + col.map(_.get(i)).mkString(" | ") + "\n"
      }
      header + "\n" + line + "\n" + content + "\n" + line + "\n"
    }
  }

}