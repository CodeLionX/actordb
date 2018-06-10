package de.up.hpi.informationsystems.adbms.csv

import java.io._
import java.net.URI

import com.univocity.parsers.common.ParsingContext
import com.univocity.parsers.common.processor.{ObjectRowProcessor, RowProcessor}
import com.univocity.parsers.conversions.Conversions
import com.univocity.parsers.csv._
import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.definition._

import scala.collection.JavaConverters._
import scala.io.{BufferedSource, Codec, Source}
import scala.reflect.ClassTag

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


  implicit val fileCodec: Codec = Codec.UTF8

  val withHeader: Boolean = true
  val delimiter: Char = ','
  val lineSeparator: String = "\n"

  val format: CsvFormat = {
    val csvFormat = new CsvFormat()
    csvFormat.setQuote('"')
    csvFormat.setQuoteEscape('\\')
    csvFormat.setCharToEscapeQuoteEscaping('\\')
    csvFormat.setLineSeparator(lineSeparator)
    csvFormat.setDelimiter(delimiter)
    csvFormat
  }

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


  private def csvReader: CsvParser = {
    val settings = new CsvParserSettings()
    settings.setFormat(format)
    settings.setHeaderExtractionEnabled(true)

    val parser = new CsvParser(settings)
    parser
  }

  private def csvWriter(out: Writer): CsvWriter = {
    val settings = new CsvWriterSettings()
    settings.setFormat(format)
    settings.setHeaderWritingEnabled(true)
    new CsvWriter(out, settings)
  }

  private def readFile[T](file: File)(handler: BufferedReader => T): T = {
    val source = Source.fromFile(file)
    try {
      handler(source.bufferedReader())
    } finally {
      source.close()
    }
  }

  private def writeFile[T](file: File)(handler: BufferedWriter => Unit): Unit = {
    val source = new BufferedWriter(new FileWriter(file))
    try {
      handler(source)
    } finally {
      source.close()
    }
  }

  def writeToFile(file: File, relation: Relation): Unit = {
    writeFile(file){ out =>
      val writer = csvWriter(out)
      writer.writeHeaders(relation.columns.toSeq.asJava)
      relation.records.getOrElse(Seq.empty).foreach( record => {
        val values = relation.columns.toSeq.map(col =>
          record(col)
        )
        writer.writeRow(values.asJava)
      })
      writer.close()
    }
  }

  def readFromFile(file: File, columns: Set[UntypedColumnDef]): Relation = {
    val records = readFile(file) { in =>
      val reader = csvReader
      val lineIterator = reader.parseAllRecords(in).asScala
      val result = lineIterator.map( record =>
        Record(
          columns.map(colDef =>
            colDef -> record.getValue[colDef.value](colDef.name, classOf[colDef.value])
          ).toMap
        )
      )
      reader.stopParsing()
      result
    }
    Relation(records)
  }

  def main(args: Array[String]): Unit = {
    val file: File = new File("test.txt")
    CSVParser.writeToFile(file, CSVParser.cartPurchases)
  }
}
