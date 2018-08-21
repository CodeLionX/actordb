package de.up.hpi.informationsystems.adbms

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import de.up.hpi.informationsystems.adbms.definition.ColumnDef
import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation.Relation
import spray.json._

import scala.util.{Failure, Success}


trait JsonSupport extends SprayJsonSupport {
  import DefaultJsonProtocol._

  implicit object AnyJsonFormat extends JsonFormat[Any] {
    override def write(obj: Any): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Any = json.toString()
  }
  implicit object ColumnDefJsonReader extends JsonReader[UntypedColumnDef] {
    override def read(json: JsValue): UntypedColumnDef = json match {
      case JsObject(fields) =>
        new ColumnDef[Any](fields("name").convertTo[String], fields("default").convertTo[Any])
      case _ => deserializationError("ColumnDef expected")
    }
  }

  implicit object ColumnDefJsonWriter extends JsonWriter[UntypedColumnDef] {
    override def write(obj: UntypedColumnDef): JsValue =
      JsObject(
        "name" -> JsString(obj.name),
        "default" -> obj.default.toJson
      )
  }

  implicit object RecordJsonFormat extends RootJsonFormat[Record] {
    override def write(obj: Record): JsValue =
      JsObject(
        "columns" -> JsArray(obj.columns.map(_.toJson).toVector),
        "data" -> obj.values.toJson
      )

    override def read(json: JsValue): Record = json match {
      case JsObject(fields) =>
        val data = fields("data") match {
          case JsArray(elements) => elements.map(_.convertTo[Any])
          case _ => deserializationError("Array of values expected")
        }
        val columns = fields("columns") match {
          case JsArray(elements) => elements.map(_.convertTo[ColumnDef[Any]]).toSet
          case _ => deserializationError("Array of columns expected")
        }
        new Record(columns.zip(data).toMap)
      case _ => deserializationError("Record expected")
    }
  }

  implicit object RelationJsonFormat extends RootJsonFormat[Relation] {
    override def write(obj: Relation): JsValue = obj.records match {
      case Success(records) => JsObject(
        "failure" -> JsBoolean(false),
        "columns" -> JsArray(obj.columns.map(_.toJson).toVector),
        "data" -> JsArray(records.map( record => JsArray(record.values.toJson) ).toVector)
      )
      case Failure(_) => JsObject(
        "failure" -> JsBoolean(true),
        "columns" -> JsArray(),
        "data" -> JsArray()
      )
    }

    override def read(json: JsValue): Relation = json match {
      case JsObject(fields) =>
        if(fields("failure").convertTo[Boolean]) {
          Relation(Failure(new Throwable("Unkown error!")))
        } else {
          val columns = fields("columns") match {
            case JsArray(elements) => elements.map(_.convertTo[ColumnDef[Any]])
            case _ => deserializationError("Array of columns expected")
          }
          val data = fields("data") match {
            case JsArray(elements) => elements.map {
              case JsArray(elements2) => elements2.map(_.convertTo[Any])
              case _ => deserializationError("Array of values expected")
            }
            case _ => deserializationError("Array of data vectors expected")
          }
          val recordBuilder = Record.fromVector(columns) _
          Relation(data.map(recordBuilder))
        }
      case _ => deserializationError("Relation expected")
    }
  }
}
