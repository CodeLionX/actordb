package de.up.hpi.informationsystems.adbms.definition

object ColumnRelation {

  //def apply(columnDef: ColumnDef): ColumnRelationDef = new ColumnRelationDef(Seq(columnDef))

  //def apply(columnDef: ColumnDef, columnDefs: ColumnDef*): ColumnRelationDef = new ColumnRelationDef(Seq(columnDef) ++ columnDefs.toSeq)

  def apply(columnDefs: Seq[ColumnDef]): ColumnRelation = new ColumnRelationImpl(columnDefs)

  class ColumnNotFoundException(message: String) extends Exception(message) {
    def this(message: String, cause: Throwable) = {
      this(message)
      initCause(cause)
    }

    def this(cause: Throwable) = this(cause.toString, cause)

    def this() = this(null: String)
  }
}

sealed trait ColumnRelation {
  protected val colMap: Map[String, ColumnDef]

  def columns: Seq[ColumnDef] = colMap.values.toSeq

  def insert[T](column: TypedColumnDef[T], value: T): Unit
  def getCol[T](column: TypedColumnDef[T]): Option[TypedColumn[T]]
}


private final class ColumnRelationImpl(private val colDefs: Seq[ColumnDef]) extends ColumnRelation {
  import ColumnRelation._

  protected override val colMap: Map[String, ColumnDef] =
    colDefs.map(c => Map(c.name -> c)).reduce(_ ++ _)

  private val data: Map[ColumnDef, Column] =
    colDefs.map { colDef: ColumnDef =>
      Map(colDef -> colDef.build())
    }.reduce(_ ++ _)
  private var n: Int = 0

  @throws[ColumnNotFoundException]
  override def insert[T](column: TypedColumnDef[T], value: T): Unit =
    if(data.contains(column)) {
      val col = data(column).asInstanceOf[TypedColumn[T]]
      col.append(value)
      if(col.length - 1 > n) n = col.length - 1
    } else
      throw new ColumnNotFoundException(s"Column $column is not part of this relation")

  override def getCol[T](column: TypedColumnDef[T]): Option[TypedColumn[T]] =
    if(data.contains(column))
      Some(data(column).asInstanceOf[TypedColumn[T]])
    else
      None

  override def toString: String = {
    val header = columns.map{c => s"${c.name}[${c.tpe}]"}.mkString(" | ")
    val line = "-" * header.length
    var content: String = ""
    for(i <- 0 to n) {
      val col: Seq[Column] = columns.map(data)
      content = content + col.map(_.get(i)).mkString(" | ") + "\n"
    }
    header + "\n" + line + "\n" + content + "\n" + line + "\n"
  }
}