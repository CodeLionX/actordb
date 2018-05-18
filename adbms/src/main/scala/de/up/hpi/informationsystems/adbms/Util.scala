package de.up.hpi.informationsystems.adbms

import de.up.hpi.informationsystems.adbms.definition.{Record, UntypedColumnDef}

object Util {

  def prettyTable(columns: Set[UntypedColumnDef], data: Seq[Record]): String = {
    val header = columns.map { c => s"${c.name}[${c.tpe}]" }.mkString(" | ")
    val line = "-" * header.length
    val content = data.map(_.values.mkString(" | ")).mkString("\n")
    header + "\n" + line + "\n" + content + "\n" + line + "\n"
  }

}
