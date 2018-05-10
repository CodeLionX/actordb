package de.up.hpi.informationsystems.adbms.definition
import scala.util.Try

abstract class RowRelation extends Relation {

  private var data: Seq[Record] = Seq.empty

  /** @inheritdoc */
  override def insert(record: Record): Unit = data = data :+ record

  /** @inheritdoc */
  override def where[T](f: (ColumnDef[T], T => Boolean)): Seq[Record] =
    data.filter{ record => record.get[T](f._1).exists(f._2) }

  /** @inheritdoc */
  override def whereAll(fs: Map[UntypedColumnDef, Any => Boolean]): Seq[Record] =
    // filter all records
    data.filter{ record =>
      fs.keys
        // map over all supplied filters (key = column)
        .map { col: UntypedColumnDef =>
        /* `val rVal = record(col)` returns the value in the record for the column `col`
         * `val filterF = fs(col)` returns the filter for column `col`
         * `val res = filterF(rVal)` applies the filter to the value of the record and corresponding column,
         * returning `true` or `false`
         */
        fs(col)(record(col))
      }
        // test if all filters for this record are true
        .forall(_ == true)
    }

  /** @inheritdoc */
  override def project(columnDefs: Set[UntypedColumnDef]): Try[Seq[Record]] = Try(
    if(columnDefs.toSet subsetOf columns.toSet)
      data.map(_.project(columnDefs.toSet).get)
    else
      throw IncompatibleColumnDefinitionException(s"this relation does not contain all specified columns {$columnDefs}")
  )

  /** @inheritdoc */
  override def toString: String = {
    val header = columns.map { c => s"${c.name}[${c.tpe}]" }.mkString(" | ")
    val line = "-" * header.length
    val content = data.map(_.values.mkString(" | ")).mkString("\n")
    header + "\n" + line + "\n" + content + "\n" + line + "\n"
  }
}