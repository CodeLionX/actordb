package de.up.hpi.informationsystems.adbms.benchmarks

import java.io.File

import de.up.hpi.informationsystems.adbms.csv.CSVParser.Implicits._
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, Relation, RowRelation}
import de.up.hpi.informationsystems.sampleapp.dactors._

object RelationBenchmark extends App {
  val dataDir = "/data/loadtest/data_100_mb"

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
    var relationName = f.getCanonicalPath.split(File.separatorChar).last.split('.').head
    try {
      var tr: Relation = nameValMapping(relationName).readFromFile(f)
      var rr: MutableRelation = RowRelation(nameValMapping(relationName))
      rr.insertAll(tr.records.get)
      relationName = null
      tr = null
      rr
    } catch {
      case e: Exception => {
        println(f)
        println(e)
        throw e
      }
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
