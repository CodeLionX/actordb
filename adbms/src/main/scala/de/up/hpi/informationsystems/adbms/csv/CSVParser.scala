package de.up.hpi.informationsystems.adbms.csv

import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, Relation, RelationDef, UntypedColumnDef}

object CSVParser {
  object CartPurchases extends RelationDef {
    val sectionId: ColumnDef[Int] = ColumnDef("sec_id")
    val sessionId: ColumnDef[Int] = ColumnDef("session_id")
    val inventoryId: ColumnDef[Int] = ColumnDef("i_id")
    val quantity: ColumnDef[Int] = ColumnDef("i_quantity")
    val fixedDiscount: ColumnDef[Double] = ColumnDef("i_fixed_disc")
    val minPrice: ColumnDef[Double] = ColumnDef("i_min_price")
    val price: ColumnDef[Double] = ColumnDef("i_price")

    override val columns: Set[UntypedColumnDef] =
      Set(sectionId, sessionId, inventoryId, quantity, fixedDiscount, minPrice, price)
    override val name: String = "cart_purchases"
  }
  class NoEncoderFoundException(msg: String) extends Exception(msg)

  val withHeader: Boolean = true
  val delimiter: String = ","
  val lineSeparator: String = "\n"

  val cartPurchases = Relation(Seq(
    CartPurchases.newRecord(
      CartPurchases.sectionId ~> 14 &
        CartPurchases.sessionId ~> 32 &
        CartPurchases.inventoryId ~> 2001 &
        CartPurchases.quantity ~> 1 &
        CartPurchases.fixedDiscount ~> 0 &
        CartPurchases.minPrice ~> 40.3 &
        CartPurchases.price ~> 60.99
    ).build(),
    CartPurchases.newRecord(
      CartPurchases.sectionId ~> 12 &
        CartPurchases.sessionId ~> 32 &
        CartPurchases.inventoryId ~> 32 &
        CartPurchases.quantity ~> 4 &
        CartPurchases.fixedDiscount ~> 10.5 &
        CartPurchases.minPrice ~> 22.5 &
        CartPurchases.price ~> 32.5
    ).build()
  ))

  def toCsv(relation: Relation): String = {
    val cols = relation.columns
    val data = relation.records.getOrElse(Seq.empty)
    val header =
      if(withHeader) {
        val start = cols.head.name
        cols.foldLeft(start)( (str, col) => str + delimiter + col.name ) + lineSeparator
      } else
        ""

    data.foldLeft(header)( (stringRel, record) => {
      val start: String = convertValue(record(cols.head))
      val line: String = cols.takeRight(cols.size - 1).foldLeft(start)( (stringRec, col) => {
        val value: String = convertValue(record(col))
        stringRec + delimiter + value
      })
      stringRel + line + lineSeparator
    })
  }

  def convertValue(x: Any): String = x match {
    case b: Byte => String.valueOf(b)
    case i: Int => String.valueOf(i)
    case l: Long => String.valueOf(l)
    case c: Char => String.valueOf(c)
    case f: Float => String.valueOf(f)
    case d: Double => String.valueOf(d)
    case s: String => s
    case sth => throw new NoEncoderFoundException(s"$sth could not be converted to a string")
  }

  def main(args: Array[String]): Unit = {
    println("---")
    print(CSVParser.toCsv(CSVParser.cartPurchases))
    println("---")
  }
}
