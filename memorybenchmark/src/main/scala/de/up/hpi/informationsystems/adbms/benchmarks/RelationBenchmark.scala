package de.up.hpi.informationsystems.adbms.benchmarks

import java.io.{BufferedReader, File}
import java.time.ZonedDateTime

import com.univocity.parsers.common.record.{Record => UVRecord}
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import de.up.hpi.informationsystems.adbms.csv.CSVParser
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, Relation, RowRelation}
import de.up.hpi.informationsystems.sampleapp.dactors._

import scala.io.{Codec, Source}
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.NonFatal

object RelationBenchmark extends App {
  val dataDir = "/data/relationtest/data-10000"

  val nameValMapping = Map(
    "cart_info" -> Cart.CartInfo,
    "cart_purchases" -> Cart.CartPurchases,
    "customer_info" -> Customer.CustomerInfo,
    "passwd" -> Customer.Password,
    "store_visits" -> Customer.StoreVisits,
    "discounts" -> GroupManager.Discounts,
    "inventory" -> StoreSection.Inventory,
    "purchase_history" -> StoreSection.PurchaseHistory
  )

  def relationFromFile(f: File): Relation = {
    val readRelation = (relationName: String, f: File) => {
      println(s"Reading relation $relationName")
      Try {
        val rr: MutableRelation = RowRelation(nameValMapping(relationName))
        Parser.readIntoMutableRelation(f, rr)
        println("Finished")
        rr

      } recover {
        case NonFatal(e) =>
          println(s"Encountered $e during processing of file $f")
          e.printStackTrace()
          Relation.empty

      } getOrElse
        Relation.empty
    }

    f.getCanonicalPath.split(File.separatorChar).lastOption.flatMap(_.split('.').headOption) match {
      case Some(name) => readRelation(name, f)
      case None => Relation.empty
    }
  }


  def recursiveListFiles(d: File): List[File] = {
    val these = d.listFiles()
    these.filter(_.isFile).toList ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  // === Main ===
  val dataURL = getClass.getResource(dataDir)
  val fileList = recursiveListFiles(new File(dataURL.getPath))

  val relations = fileList.map(relationFromFile)

  println("loaded")
  while (true) {
    Thread.sleep(500)
  }

  println(relations.last)

}

object Parser {
  private implicit val fileCodec: Codec = Codec.UTF8

  private val withHeader: Boolean = true
  private val delimiter: Char = CSVParser.defaultDelimiter
  private val lineSeparator: String = CSVParser.defaultLineSeparator

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

  private def readFile[T](file: File)(handler: BufferedReader => T): T = {
    val source = Source.fromFile(file)
    val reader = source.bufferedReader()
    try {
      handler(reader)
    } finally {
      source.close()
    }
  }

  def readIntoMutableRelation(file: File, relation: MutableRelation): Unit = {
    val columns = relation.columns
    readFile(file)((in: BufferedReader) => {
      val reader = csvReader
      reader.beginParsing(in)

      var record: UVRecord = reader.parseNextRecord()
      var counter: Long = 1
      while(record != null) {
        val adbmsRecord: Record = Record.fromMap(
          columns.toSeq.map {
            case colDef if colDef.tpe == ClassTag.apply(classOf[ZonedDateTime]) => colDef -> ZonedDateTime.parse(record.getString(colDef.name))
            case colDef => colDef -> record.getValue(colDef.name, colDef.default)
          }.toMap
        )
        relation.insert(adbmsRecord)

        if(counter % 1000000 == 0) println(s"processed $counter records")
        record = reader.parseNextRecord()
        counter += 1
      }
      reader.stopParsing()
    })
  }
}
