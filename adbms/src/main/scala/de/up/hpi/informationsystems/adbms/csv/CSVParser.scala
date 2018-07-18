package de.up.hpi.informationsystems.adbms.csv

import java.io._
import java.time.ZonedDateTime

import com.univocity.parsers.csv._
import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation.Relation

import scala.collection.JavaConverters._
import scala.io.{Codec, Source}
import scala.reflect.ClassTag

object CSVParser {

  private type BufferedEncoder = BufferedWriter => Unit
  private type BufferedDecoder[T] = BufferedReader => T

  val defaultDelimiter: Char = ','
  val defaultLineSeparator: String = "\n"

  /**
    * Creates a new `CSVParser` for encoding and decoding
    * [[de.up.hpi.informationsystems.adbms.relation.Relation]]s
    * with the default configuration (delim=, and lineSep=\n).
 *
    * @return a configured `CSVParser`
    */
  def apply(): CSVParser = new CSVParser(defaultDelimiter, defaultLineSeparator)

  /**
    * Creates a new `CSVParser` for encoding and decoding
    * [[de.up.hpi.informationsystems.adbms.relation.Relation]]s
    * using the provided configuration.
 *
    * @param delim value delimiter
    * @param lineSep line separator
    * @return a configured `CSVParser`
    */
  def apply(delim: Char, lineSep: String): CSVParser =
    new CSVParser(delim, lineSep)

  /**
    * Import this object to enrich
    * [[de.up.hpi.informationsystems.adbms.relation.Relation]]s
    * and
    * [[de.up.hpi.informationsystems.adbms.definition.RelationDef]]s
    * with methods to encode and decode them as csv.
    *
    * @example
    * {{{
    * import de.up.hpi.informationsystems.adbms.csv.CSVParser.Implicits._
    *
    * val testRelation: Relation = _
    * val csvString: String = testRelation.writeToCsv
    * val readRelation: Relation = testRelation.readFromCsv(csvString)
    *
    * assert( testRelation.records == readRelation.records ) // true
    * }}}
    */
  object Implicits {
    private val parser = CSVParser()

    implicit class RichRelation(relation: Relation) {

      /**
        * Encodes this relation as csv and writes it's data to `file`
        * @param file file to write the relation's data to
        */
      def writeToFile(file: File): Unit =
        parser.writeToFile(file, relation)

      /**
        * Decodes csv data read from `file` using this relation's column definition.
        * @param file to read the relation's data from
        * @return a new relation containing the data from the csv file
        */
      def readFromFile(file: File): Relation =
        parser.readFromFile(file, relation.columns)

      /**
        * Encodes this relation as csv and writes it's data to `stream`
        * @param stream stream to write the relation's data to
        */
      def writeToStream(stream: OutputStream): Unit =
        parser.writeToStream(stream, relation)

      /**
        * Decodes csv data read from `stream` using this relation's column definition.
        * @param stream to read the relation's data from
        * @return a new relation containing the data from the csv file
        */
      def readFromStream(stream: InputStream): Relation =
        parser.readFromStream(stream, relation.columns)

      /**
        * Encodes this relation as a csv string.
        * @return a string contain this relation's data as csv
        */
      def writeToCsv: String =
        parser.writeToCsv(relation)

      /**
        * Decodes the csv string using this relation's column definition.
        * @param csv string to read the relation's data from
        * @return a new relation containing the data from the file
        */
      def readFromCsv(csv: String): Relation =
        parser.readFromCsv(csv, relation.columns)

    }

    implicit class RichRelationDef(relationDef: RelationDef) {

      /**
        * Decodes csv data read from `file` using this relationDef's column definition.
        * @param file to read the relation's data from
        * @return a new relation containing the data from the csv file
        */
      def readFromFile(file: File): Relation =
        parser.readFromFile(file, relationDef.columns)

      /**
        * Decodes csv data read from `stream` using this relationDef's column definition.
        * @param stream to read the relation's data from
        * @return a new relation containing the data from the csv file
        */
      def readFromStream(stream: InputStream): Relation =
        parser.readFromStream(stream, relationDef.columns)

      /**
        * Decodes the csv string using this relationDef's column definition.
        * @param csv string to read the relation's data from
        * @return a new relation containing the data from the file
        */
      def readFromCsv(csv: String): Relation =
        parser.readFromCsv(csv, relationDef.columns)

    }
  }

}

class CSVParser(delim: Char, lineSep: String) {
  import de.up.hpi.informationsystems.adbms.csv.CSVParser.{BufferedDecoder, BufferedEncoder}

  private implicit val fileCodec: Codec = Codec.UTF8

  private val withHeader: Boolean = true
  private val delimiter: Char = delim
  private val lineSeparator: String = lineSep

  private val format: CsvFormat = {
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
    * Loan Pattern (a.k.a. lender-lendee-pattern) for reader resource: File
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
    * Loan Pattern (a.k.a. lender-lendee-pattern) for writer resource: File
    */
  private def writeFile(file: File)(handler: BufferedEncoder): Unit = {
    val source = new BufferedWriter(new FileWriter(file))
    try {
      handler(source)
    } finally {
      source.close()
    }
  }

  /**
    * Loan Pattern (a.k.a. lender-lendee-pattern) for reader resource: Stream
    */
  private def readStream[T](stream: InputStream)(handler: BufferedDecoder[T]): T = {
    val reader = new BufferedReader(new InputStreamReader(stream))
    try {
      handler(reader)
    } finally {
      reader.close()
    }
  }

  /**
    * Loan Pattern (a.k.a. lender-lendee-pattern) for writer resource: Stream
    */
  private def writeStream(stream: OutputStream)(handler: BufferedEncoder): Unit = {
    val source = new BufferedWriter(new OutputStreamWriter(stream))
    try {
      handler(source)
    } finally {
      source.close()
    }
  }

  /**
    * Loan Pattern (a.k.a. lender-lendee-pattern) for reader resource: String
    */
  private def readString[T](str: String)(handler: BufferedDecoder[T]): T = {
    val reader = new BufferedReader(new StringReader(str))
    try {
      handler(reader)
    } finally {
      reader.close()
    }
  }

  /**
    * Loan Pattern (a.k.a. lender-lendee-pattern) for writer resource: String
    */
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

  private def decodeWithUnivocity(columns: Set[UntypedColumnDef]): BufferedDecoder[Relation] = (in: BufferedReader) => {
    val reader = csvReader
    val lineIterator = reader.parseAllRecords(in).asScala
    val result = lineIterator.map( record =>
      Record.fromMap(
        columns.toSeq.map {
          case colDef if colDef.tpe == ClassTag.apply(classOf[ZonedDateTime]) => colDef -> ZonedDateTime.parse(record.getString(colDef.name))
          case colDef => colDef -> record.getValue(colDef.name, colDef.default)
        }.toMap
      )
    )
    reader.stopParsing()
    Relation(result)
  }

  private def encodeWithUnicotiy(relation: Relation): BufferedEncoder = (out: BufferedWriter) => {
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

  // public API

  /**
    * Writes the contents of `relation` to the file `file` as csv.
    * @param file instance to write the csv string to
    * @param relation relation, which's records will be decoded to csv
    */
  def writeToFile(file: File, relation: Relation): Unit =
    writeFile(file)(encodeWithUnicotiy(relation))

  /**
    * Returns a new relation, which's data is read from a csv file using
    * the provided column definitions to decode the data.
    * @param file file to read the relation's data from
    * @param columns used to decode the csv string (header, type and default value information)
    * @return a new relation containing the contents of the decoded csv file
    */
  def readFromFile(file: File, columns: Set[UntypedColumnDef]): Relation =
    readFile(file)(decodeWithUnivocity(columns))

  /**
    * Writes the contents of `relation` to the stream `stream` as csv.
    * @param stream output stream to write to
    * @param relation relation, which's records will be decoded to csv
    */
  def writeToStream(stream: OutputStream, relation: Relation): Unit =
    writeStream(stream)(encodeWithUnicotiy(relation))

  /**
    * Returns a new relation, which's data is read from the input stream in csv format
    * using the provided column definitions to decode the data.
    * @param stream stream to read the relation's data from
    * @param columns used to decode the csv string (header, type and default value information)
    * @return a new relation containing the contents of the decoded stream
    */
  def readFromStream(stream: InputStream, columns: Set[UntypedColumnDef]): Relation =
    readStream(stream)(decodeWithUnivocity(columns))

  /**
    * Returns the contents of `relation` as csv string.
    * @param relation relation, which's columns will be decoded to csv
    * @return `relation`s data as csv string
    */
  def writeToCsv(relation: Relation): String =
    writeString(encodeWithUnicotiy(relation))

  /**
    * Returns a new relation, which's data is read from the csv string using
    * the provided column definitions to decode the data.
    * @param csv string to read the relation's data from
    * @param columns used to decode the csv string (header, type and default value information)
    * @return a new relation containing the contents of the decoded csv file
    */
  def readFromCsv(csv: String, columns: Set[UntypedColumnDef]): Relation =
    readString(csv)(decodeWithUnivocity(columns))
}
