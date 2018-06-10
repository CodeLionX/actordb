package de.up.hpi.informationsystems.adbms.csv

import java.io.{BufferedReader, File, FileNotFoundException, FileReader}
import java.net.URI

import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, Relation, RelationDef, UntypedColumnDef}
import org.scalatest.{Matchers, WordSpec}

import scala.io.Source

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

  val testRelation = Relation(Seq(
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
  val importFilename: String = "imported.csv"
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

    "successfully export a relation to a csv file" in {
      val file = getFile(exportFilename)
      parser.writeToFile(file, testRelation)

      val source = Source.fromFile(file)
      source.mkString shouldEqual Seq(
        "intCol,charCol,byteCol,doubleCol,longCol,shortCol,stringCol,floatCol,boolCol\n",
        "3,x,1,6.0,4,2,test,5.0,true\n",
        "0,,0,0.0,0,0,,0.0,false\n",
        "30,a,10,6.6,40,20,bla,5.5,true\n"
      ).mkString
      source.close()
    }

    "successfully import a relation from a csv file" in {
      val file = getFile(importFilename)
      val relation = parser.readFromFile(file, TestRelation.columns)

      relation.records shouldEqual testRelation.records
    }

    "provide implicits for a relation" in {
      import CSVParser.Implicits._

      val file = getFile("implicitTest.csv")
      testRelation.writeToFile(file)

      val readRelation = testRelation.readFromFile(file)
      val readRelationDef = TestRelation.readFromFile(file)

      testRelation.records shouldEqual readRelation.records
      testRelation.records shouldEqual readRelationDef.records
    }
  }

}
