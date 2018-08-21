package de.up.hpi.informationsystems.fouleggs.httpserver

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import de.up.hpi.informationsystems.adbms
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation.Relation
import de.up.hpi.informationsystems.fouleggs.movieScoringService.movies.Movie
import spray.json._

trait JsonSupport extends SprayJsonSupport with adbms.JsonSupport {
  import DefaultJsonProtocol._

  implicit object MovieJsonFormat extends RootJsonFormat[Movie] {
    override def write(obj: Movie): JsValue =
      JsObject(
        "info" -> obj.info.toJson,
        "trailer" -> obj.trailer.toJson,
        "cast" -> obj.cast.toJson,
        "studio" -> obj.studio.toJson
      )

    override def read(json: JsValue): Movie = json match {
      case JsObject(fields) =>
        Movie(
          fields("info").convertTo[Record],
          fields("trailer").convertTo[Relation],
          fields("cast").convertTo[Relation],
          fields("studio").convertTo[Record]
        )
    }
  }
}
