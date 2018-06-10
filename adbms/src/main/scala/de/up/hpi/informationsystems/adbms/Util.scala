package de.up.hpi.informationsystems.adbms

import de.up.hpi.informationsystems.adbms.definition.{Record, UntypedColumnDef}

object Util {

  def prettyTable(columns: Set[UntypedColumnDef], data: Seq[Record]): String = {
    val orderedColumns = columns.toSeq
    val header = orderedColumns.map { c => s"${c.name}[${c.tpe}]" }.mkString(" | ")
    val line = "-" * header.length
    val content = data.map( record =>
      orderedColumns.map( col =>
        record(col)
      ).mkString(" | ")
    ).mkString("\n")
    header + "\n" + line + "\n" + content + "\n" + line + "\n"
  }

}
