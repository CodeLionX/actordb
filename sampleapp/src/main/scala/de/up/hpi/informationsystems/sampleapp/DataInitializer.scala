package de.up.hpi.informationsystems.sampleapp

import java.io.File
import java.net.URI

import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.csv.CSVParser
import de.up.hpi.informationsystems.adbms.definition.RelationDef

object DataInitializer {

  final case class LoadData(rootPath: URI)

}

trait DataInitializer extends Dactor {
  import DataInitializer._

  private val csvParser = CSVParser()

  private def getFile(root: URI, dactorName: String, relationDef: RelationDef): File = {
    val fileName = s"${root.toString}/$name/${relationDef.name}.csv"
    log.info(s"Try loading file $fileName")
    new File(
      new URI(fileName)
    )
//    getClass.getResourceAsStream()
  }

  private def handleRequest: Receive = {
    case LoadData(rootPath) =>
      log.info(s"received LoadData message with path $rootPath")
      try {
        relations.foreach { case (relationDef, relation) =>
          val file = getFile(rootPath, name, relationDef)
          val csvData = csvParser.readFromFile(file, relationDef.columns)
          relation.insertAll(csvData.records.get)
        }
        sender() ! akka.actor.Status.Success
      } catch {
        case e: Throwable => sender() ! akka.actor.Status.Failure(e)
      }
  }

  abstract override def receive: Receive = handleRequest orElse super.receive

}
