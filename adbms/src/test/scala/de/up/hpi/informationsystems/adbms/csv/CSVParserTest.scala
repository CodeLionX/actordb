package de.up.hpi.informationsystems.adbms.csv

import java.io._
import java.net.URI

import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, Relation, RelationDef, UntypedColumnDef}
import org.scalatest.{Matchers, WordSpec}


object CSVParserTest {
  
  object TestRelation extends RelationDef {
    val byteCol: ColumnDef[Byte] = ColumnDef("byteCol")
    val shortCol: ColumnDef[Short] = ColumnDef("shortCol")
    val intCol: ColumnDef[Int] = ColumnDef("intCol")
    val longCol: ColumnDef[Long] = ColumnDef("longCol")
    val floatCol: ColumnDef[Float] = ColumnDef("floatCol")
    val doubleCol: ColumnDef[Double] = ColumnDef("doubleCol")
    val charCol: ColumnDef[Char] = ColumnDef("charCol")
    val stringCol: ColumnDef[String] = ColumnDef("stringCol")
    val boolCol: ColumnDef[Boolean] = ColumnDef("boolCol")

    override val columns: Set[UntypedColumnDef] =
      Set(byteCol, shortCol, intCol, longCol, floatCol, doubleCol, charCol, stringCol, boolCol)
    override val name: String = "TestRelation"
  }

  val testRelation: Relation = Relation(Seq(
    TestRelation.newRecord(
      TestRelation.byteCol ~> 1.toByte &
      TestRelation.shortCol ~> 2.toShort &
      TestRelation.intCol ~> 3 &
      TestRelation.longCol ~> 4L &
      TestRelation.floatCol ~> 5.0f &
      TestRelation.doubleCol ~> 6.0 &
      TestRelation.charCol ~> 'x' &
      TestRelation.stringCol ~> "test" &
      TestRelation.boolCol ~> true
    ).build(),
    TestRelation.newRecord.build(),
    TestRelation.newRecord(
      TestRelation.byteCol ~> 10.toByte &
      TestRelation.shortCol ~> 20.toShort &
      TestRelation.intCol ~> 30 &
      TestRelation.longCol ~> 40L &
      TestRelation.floatCol ~> 5.5f &
      TestRelation.doubleCol ~> 6.6 &
      TestRelation.charCol ~> 'a' &
      TestRelation.stringCol ~> "bla" &
      TestRelation.boolCol ~> true
    ).build()
  ))

  val exportFilename: String = "exported.csv"
}

class CSVParserTest extends WordSpec with Matchers {
  import CSVParserTest._

  def getFile(address: String): File = {
    try {
      val uri = getClass
        .getResource("/")
        .toString
      new File(new URI(uri + address))
    } catch {
      case e: Throwable => throw new FileNotFoundException(s"$address could not be found").initCause(e)
    }
  }

  "A CSVParser" should {
    val parser = CSVParser()

    "successfully export and import a relation using csv file" in {
      val file = getFile(exportFilename)
      parser.writeToFile(file, testRelation)
      val relation = parser.readFromFile(file, TestRelation.columns)

      relation.records shouldEqual testRelation.records
    }

    "successfully overwrite existing file when exporting relation to a csv file" in {
      val file = getFile(exportFilename)
      parser.writeToFile(file, testRelation)
      parser.writeToFile(file, testRelation)  // second time around
      val relation = parser.readFromFile(file, TestRelation.columns)

      relation.records shouldEqual testRelation.records
    }

    "successfully overwrite existing data in relation when loading data from a csv file" in {
      import CSVParser.Implicits._

      val file = getFile(exportFilename)
      parser.writeToFile(file, testRelation)
      val relation = parser.readFromFile(file, TestRelation.columns)
      relation.readFromFile(file)   // second time around

      relation.records shouldEqual testRelation.records
    }

    "provide implicits for a relation to process csv files" in {
      import CSVParser.Implicits._

      val file = getFile("implicitTest.csv")
      testRelation.writeToFile(file)

      val readRelation = testRelation.readFromFile(file)
      val readRelationDef = TestRelation.readFromFile(file)

      testRelation.records shouldEqual readRelation.records
      testRelation.records shouldEqual readRelationDef.records
    }

    "successfully export and import a relation using csv string" in {
      val csvString = parser.writeToCsv(testRelation)
      val relation = parser.readFromCsv(csvString, TestRelation.columns)

      relation.records shouldEqual testRelation.records
    }

    "provide implicits for a relation to process csv strings" in {
      import CSVParser.Implicits._

      val csvString = testRelation.writeToCsv

      val readRelation = testRelation.readFromCsv(csvString)
      val readRelationDef = TestRelation.readFromCsv(csvString)

      testRelation.records shouldEqual readRelation.records
      testRelation.records shouldEqual readRelationDef.records
    }

    "successfully export and import a relation using csv streams" in {
      val outputStream = new ByteArrayOutputStream()
      parser.writeToStream(outputStream, testRelation)
      val inputStream = new ByteArrayInputStream(outputStream.toByteArray)
      val relation = parser.readFromStream(inputStream, TestRelation.columns)

      relation.records shouldEqual testRelation.records
    }

    "provide implicits for a relation to process csv streams" in {
      import CSVParser.Implicits._

      val outputStream = new ByteArrayOutputStream()
      testRelation.writeToStream(outputStream)

      val inputArray = outputStream.toByteArray
      val readRelation = testRelation.readFromStream(new ByteArrayInputStream(inputArray))
      val readRelationDef = TestRelation.readFromStream(new ByteArrayInputStream(inputArray))

      testRelation.records shouldEqual readRelation.records
      testRelation.records shouldEqual readRelationDef.records
    }
  }

}
