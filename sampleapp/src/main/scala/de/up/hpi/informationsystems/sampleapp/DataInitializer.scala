package de.up.hpi.informationsystems.sampleapp

import java.io.InputStream

import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.csv.CSVParser
import de.up.hpi.informationsystems.adbms.definition.RelationDef

object DataInitializer {

  final case class LoadData(rootPath: String)

}

/**
  * Mixin for [[de.up.hpi.informationsystems.adbms.Dactor]]s to automatically
  * load data from csv files into the `Dactor`'s relations when a
  * [[de.up.hpi.informationsystems.sampleapp.DataInitializer.LoadData]] request
  * is received.
  *
  * @example
  * {{{
  * class MyDactorBase extends Dactor {
  *   override protected val relations: Map[RelationDef, MutableRelation] = ...
  *   override def receive: Receive = ...
  * }
  * class MyDactor extends MyDactorBase with DataInitializer
  * }}}
  */
trait DataInitializer extends Dactor {
  import DataInitializer._

  private val csvParser = CSVParser()

  abstract override def receive: Receive = handleRequest orElse super.receive

  private def handleRequest: Receive = {
    case LoadData(rootPath) =>
      log.info(s"received LoadData message with path $rootPath")
      try {
        relations.foreach { case (relationDef, relation) =>
          val inputStream = getResourceInputStream(rootPath, name, relationDef)
          val csvData = csvParser.readFromStream(inputStream, relationDef.columns)
          relation.insertAll(csvData.records.get)
        }
        sender() ! akka.actor.Status.Success
      } catch {
        case e: Throwable => sender() ! akka.actor.Status.Failure(e)
      }
  }

  private def getResourceInputStream(root: String, dactorName: String, relationDef: RelationDef): InputStream = {
    val fileName = s"$root/$name/${relationDef.name}.csv"
    log.info(s"Try loading file $fileName")
    getClass.getResourceAsStream(fileName)
  }
}
