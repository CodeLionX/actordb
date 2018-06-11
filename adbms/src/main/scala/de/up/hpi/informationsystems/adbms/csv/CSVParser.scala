package de.up.hpi.informationsystems.adbms.csv

import java.io._

import com.univocity.parsers.csv._
import de.up.hpi.informationsystems.adbms.definition._

import scala.collection.JavaConverters._
import scala.io.{Codec, Source}

object CSVParser {

  private type BufferedEncoder = BufferedWriter => Unit
  private type BufferedDecoder[T] = BufferedReader => T

  val defaultDelimiter: Char = ','
  val defaultLineSeparator: String = "\n"

  def apply(): CSVParser = new CSVParser(defaultDelimiter, defaultLineSeparator)

  def apply(delim: Char, lineSep: String): CSVParser =
    new CSVParser(delim, lineSep)

  object Implicits {
    private val parser = CSVParser()

    implicit class RichRelation(relation: Relation) {

      def writeToFile(file: File): Unit =
        parser.writeToFile(file, relation)

      def readFromFile(file: File): Relation =
        parser.readFromFile(file, relation.columns)

      def writeToCsv: String =
        parser.writeToCsv(relation)

      def readFromCsv(csv: String): Relation =
        parser.readFromCsv(csv, relation.columns)

    }

    implicit class RichRelationDef(relationDef: RelationDef) {

      def readFromFile(file: File): Relation =
        parser.readFromFile(file, relationDef.columns)

      def readFromCsv(csv: String): Relation =
        parser.readFromCsv(csv, relationDef.columns)

    }
  }

}

class CSVParser(delim: Char, lineSep: String) {
  import de.up.hpi.informationsystems.adbms.csv.CSVParser.{BufferedDecoder, BufferedEncoder}

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
  private def readFile[T](file: File)(handler: BufferedDecoder[T]): T = {
    val source = Source.fromFile(file)
    val reader = source.bufferedReader()
    try {
      handler(reader)
    } finally {
      source.close()
    }
  }

  /**
    * Loan Pattern (a.k.a. lender-lendee-pattern) for writer resource.
    */
  private def writeFile(file: File)(handler: BufferedEncoder): Unit = {
    val source = new BufferedWriter(new FileWriter(file))
    try {
      handler(source)
    } finally {
      source.close()
    }
  }

  private def readString[T](str: String)(handler: BufferedDecoder[T]): T = {
    val reader = new BufferedReader(new StringReader(str))
    try {
      handler(reader)
    } finally {
      reader.close()
    }
  }

  private def writeString(handler: BufferedEncoder): String = {
    val source = new StringWriter()
    val writer = new BufferedWriter(source)
    try{
      handler(writer)
      source.toString
    } finally {
      source.close()
    }
  }

  def decodeWithUnivocity(columns: Set[UntypedColumnDef]): BufferedDecoder[Relation] = (in: BufferedReader) => {
    val reader = csvReader
    val lineIterator = reader.parseAllRecords(in).asScala
    val result = lineIterator.map( record =>
      Record.fromMap(
        columns.toSeq.map(colDef =>
          colDef -> record.getValue(colDef.name, colDef.default)
        ).toMap
      )
    )
    reader.stopParsing()
    Relation(result)
  }

  def encodeWithUnicotiy(relation: Relation): BufferedEncoder = (out: BufferedWriter) => {
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

  def writeToFile(file: File, relation: Relation): Unit =
    writeFile(file)(encodeWithUnicotiy(relation))

  def readFromFile(file: File, columns: Set[UntypedColumnDef]): Relation =
    readFile(file)(decodeWithUnivocity(columns))

  def writeToCsv(relation: Relation): String =
    writeString(encodeWithUnicotiy(relation))

  def readFromCsv(csv: String, columns: Set[UntypedColumnDef]): Relation =
    readString(csv)(decodeWithUnivocity(columns))
}
