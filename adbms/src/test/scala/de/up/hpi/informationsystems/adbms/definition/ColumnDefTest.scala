package de.up.hpi.informationsystems.adbms.definition

import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._
import org.scalatest.{Matchers, WordSpec}

import scala.reflect.classTag

class ColumnDefTest extends WordSpec with Matchers {

  "A ColumnDef" should {
    val name = "name"
    val colInt = ColumnDef[Int]("integerColumn", 0)

    "support all basic data types and infer their default value" in {
      ColumnDef[Byte](name, 0.toByte) should equal (ColumnDef[Byte](name))
      ColumnDef[Short](name, 0.toShort) should equal (ColumnDef[Short](name))
      ColumnDef[Int](name, 0) should equal (ColumnDef[Int](name))
      ColumnDef[Long](name, 0L) should equal (ColumnDef[Long](name))
      ColumnDef[Float](name, 0.0f) should equal (ColumnDef[Float](name))
      ColumnDef[Double](name, 0.0) should equal (ColumnDef[Double](name))
      ColumnDef[Char](name, '\0') should equal (ColumnDef[Char](name))
      ColumnDef[String](name, "") should equal (ColumnDef[String](name))
      ColumnDef[Boolean](name, false) should equal (ColumnDef[Boolean](name))
      ColumnDef[Any](name, null) should equal (ColumnDef[Any](name, null))
      ColumnDef[AnyRef](name, null) should equal (ColumnDef[AnyRef](name, null))

      ColumnDef[BigInt](name, null) should equal (ColumnDef[BigInt](name))
      ColumnDef[BigDecimal](name, null) should equal (ColumnDef[BigDecimal](name))

      // for those i'm not sure:
      ColumnDef[Unit](name, ()) should equal (ColumnDef[Unit](name))
      ColumnDef[Nothing](name, _: Nothing) should equal (ColumnDef[Nothing](name))
    }

    "support more complex data types" in {
      // do we want to prevent this or not?
      case class MyComplexType(s: String, i: Int, f: Float, b: Boolean)
      class SomeType
      type testType = () => Seq[String] => (Int, Int)

      ColumnDef[Seq[Any]](name, Seq.empty) should equal (ColumnDef[Seq[Any]](name))
      ColumnDef[MyComplexType](name, null) should equal (ColumnDef[MyComplexType](name))
      ColumnDef[SomeType](name, null) should equal (ColumnDef[SomeType](name))
      ColumnDef[Array[Map[Int, Any]]](name, null) should equal (ColumnDef[Array[Map[Int, Any]]](name))
      ColumnDef[String => (Int, Int)](name, null) should equal (ColumnDef[String => (Int, Int)](name))
      ColumnDef[testType](name, null) should equal (ColumnDef[testType](name))
      // insert arbitrary complex types here
    }

    "equal another ColumnDef if and only if name, type and default value are equal" in {
      colInt should equal (ColumnDef[Int]("integerColumn"))
      colInt should equal (ColumnDef[Int]("integerColumn", 0))
      colInt should equal (ColumnDef[Int]("integerColumn").untyped)
      colInt.untyped should equal (ColumnDef[Int]("integerColumn"))
      colInt.untyped should equal (ColumnDef[Int]("integerColumn", 0))
      colInt.untyped should equal (ColumnDef[Int]("integerColumn").untyped)
    }

    "not equal if either name or type or both are not equal" in {
      colInt shouldNot equal (ColumnDef[Any]("integerColumn", null))
      colInt shouldNot equal (ColumnDef[Byte]("integerColumn"))
      colInt shouldNot equal (ColumnDef[Int]("differentName"))
      colInt shouldNot equal (ColumnDef[Int]("integerColumn", 12))
      colInt shouldNot equal (null)

      colInt.untyped shouldNot equal (ColumnDef[Any]("integerColumn", null).untyped)
      colInt.untyped shouldNot equal (ColumnDef[Byte]("integerColumn").untyped)
      colInt.untyped shouldNot equal (ColumnDef[Int]("differentName").untyped)
      colInt.untyped shouldNot equal (ColumnDef[Int]("integerColumn", 12).untyped)
      colInt.untyped shouldNot equal (null)
    }

    "provide a hash that uniquely identifies its name, type and default value" in {
      colInt.hashCode() should equal(ColumnDef[Int]("integerColumn").hashCode())
      colInt.hashCode() should equal(ColumnDef[Int]("integerColumn", 0).hashCode())
      colInt.hashCode() should equal(ColumnDef[Int]("integerColumn").untyped.hashCode())
      colInt.hashCode() shouldNot equal(ColumnDef[Byte]("integerColumn").hashCode())
      colInt.hashCode() shouldNot equal(ColumnDef[Int]("differentName").hashCode())
      colInt.hashCode() shouldNot equal(ColumnDef[Int]("integerColumn", 12).hashCode())

      colInt.untyped.hashCode() should equal(ColumnDef[Int]("integerColumn").untyped.hashCode())
      colInt.untyped.hashCode() should equal(ColumnDef[Int]("integerColumn").hashCode())
      colInt.untyped.hashCode() shouldNot equal(ColumnDef[Byte]("integerColumn").untyped.hashCode())
      colInt.untyped.hashCode() shouldNot equal(ColumnDef[Int]("differentName").untyped.hashCode())
      colInt.untyped.hashCode() shouldNot equal(ColumnDef[Int]("integerColumn", 12).untyped.hashCode())
    }

    "contain its type in form of a classTag" in {
      colInt.tpe should equal(classTag[Int])
      colInt.tpe shouldNot equal(classTag[Byte])

      // tpe can be used to filter columns
      val columns: Seq[UntypedColumnDef] = Seq(ColumnDef[Int]("int"), ColumnDef[String]("string"), ColumnDef[Int]("int2"))
      def extract(colDef: UntypedColumnDef): Option[String] = {
        colDef match {
          case i if i.tpe == classTag[Int] => Some(i.name)
          case _ => None
        }
      }
      columns.flatMap(extract) should equal(Seq("int", "int2"))
    }

  }
}
