package de.up.hpi.informationsystems.adbms.csv

import java.io._
import java.net.URI

import com.univocity.parsers.common.ParsingContext
import com.univocity.parsers.common.processor.{ObjectRowProcessor, RowProcessor}
import com.univocity.parsers.conversions.Conversions
import com.univocity.parsers.csv._
import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._
import de.up.hpi.informationsystems.adbms.definition._

import scala.collection.JavaConverters._
import scala.io.{BufferedSource, Codec, Source}
import scala.reflect.ClassTag

object CSVParser {

  val defaultDelimiter: Char = ','
  val defaultLineSeparator: String = "\n"

  def apply(): CSVParser = new CSVParser(defaultDelimiter, defaultLineSeparator)

  def apply(delim: Char, lineSep: String): CSVParser =
    new CSVParser(delim, lineSep)

}

class CSVParser(delim: Char, lineSep: String) {

  implicit val fileCodec: Codec = Codec.UTF8

  val withHeader: Boolean = true
  val delimiter: Char = delim
  val lineSeparator: String = lineSep

  val format: CsvFormat = {
    val csvFormat = new CsvFormat()
    csvFormat.setQuote('"')
    csvFormat.setQuoteEscape('\\')
    csvFormat.setCharToEscapeQuoteEscaping('\\')
    csvFormat.setLineSeparator(lineSeparator)
    csvFormat.setDelimiter(delimiter)
    csvFormat
  }

  private def csvReader: CsvParser = {
    val settings = new CsvParserSettings()
    settings.setFormat(format)
    settings.setHeaderExtractionEnabled(withHeader)
    new CsvParser(settings)
  }

  private def csvWriter(out: Writer): CsvWriter = {
    val settings = new CsvWriterSettings()
    settings.setFormat(format)
    settings.setHeaderWritingEnabled(withHeader)
    new CsvWriter(out, settings)
  }

  /**
    * Loan Pattern (a.k.a. lender-lendee-pattern) for reader resource.
    */
  private def readFile[T](file: File)(handler: BufferedReader => T): T = {
    val source = Source.fromFile(file)
    try {
      handler(source.bufferedReader())
    } finally {
      source.close()
    }
  }

  /**
    * Loan Pattern (a.k.a. lender-lendee-pattern) for writer resource.
    */
  private def writeFile[T](file: File)(handler: BufferedWriter => Unit): Unit = {
    val source = new BufferedWriter(new FileWriter(file))
    try {
      handler(source)
    } finally {
      source.close()
    }
  }

  def writeToFile(file: File, relation: Relation): Unit =
    writeFile(file){ out =>
      val writer = csvWriter(out)
      val orderedColumns = relation.columns.toSeq

      writer.writeHeaders(orderedColumns.map(_.name).asJava)

      relation.records.getOrElse(Seq.empty).foreach( record => {
        val values = orderedColumns.map(col =>
          record(col)
        )
        writer.writeRow(values.asJava)
      })
      writer.close()
    }

  def readFromFile(file: File, columns: Set[UntypedColumnDef]): Relation = {
    val records = readFile(file) { in =>
      val reader = csvReader
      val lineIterator = reader.parseAllRecords(in).asScala
      val result = lineIterator.map( record =>
        Record.fromMap(
          columns.map(colDef =>
            colDef -> record.getValue(colDef.name, colDef.default)
          ).toMap
        )
      )
      reader.stopParsing()
      result
    }
    Relation(records)
  }
}
