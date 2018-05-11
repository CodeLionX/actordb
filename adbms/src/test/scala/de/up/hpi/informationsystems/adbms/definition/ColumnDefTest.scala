package de.up.hpi.informationsystems.adbms.definition

import org.scalatest.WordSpec

import scala.reflect.{ClassTag, classTag}

class ColumnDefTest extends WordSpec {

  "A ColumnDef" should {
    val colInt = ColumnDef[Int]("integerColumn")

    "equal another ColumnDef only if name and type are equal" in {
      assert(colInt == ColumnDef[Int]("integerColumn"))
      assert(colInt != ColumnDef[Byte]("integerColumn"))
      assert(colInt != ColumnDef[Int]("differentName"))
      assert(colInt != null)

      assert(colInt.untyped == ColumnDef[Int]("integerColumn").untyped)
      assert(colInt.untyped != ColumnDef[Byte]("integerColumn").untyped)
      assert(colInt.untyped != ColumnDef[Int]("differentName").untyped)
      assert(colInt.untyped != null)
    }

    "provide a hash that uniquely identifies its name and type" in {
      assert(colInt.hashCode() == ColumnDef[Int]("integerColumn").hashCode())
      assert(colInt.hashCode() == ColumnDef[Int]("integerColumn").untyped.hashCode())
      assert(colInt.hashCode() != ColumnDef[Byte]("integerColumn").hashCode())
      assert(colInt.hashCode() != ColumnDef[Int]("differentName").hashCode())

      assert(colInt.untyped.hashCode() == ColumnDef[Int]("integerColumn").untyped.hashCode())
      assert(colInt.untyped.hashCode() == ColumnDef[Int]("integerColumn").hashCode())
      assert(colInt.untyped.hashCode() != ColumnDef[Byte]("integerColumn").untyped)
      assert(colInt.untyped.hashCode() != ColumnDef[Int]("differentName").untyped)
    }

    "provide clones of itself" in {
      val cloneColInt = colInt.clone()

      assert(colInt ne cloneColInt)
      assert(colInt == cloneColInt)
    }

    "contain its type in form of a classTag" in {
      assert(colInt.tpe == classTag[Int])
      assert(colInt.tpe != classTag[Byte])
    }

  }
}
