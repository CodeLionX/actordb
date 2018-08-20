package de.up.hpi.informationsystems.adbms.benchmarks

import java.io.File

import de.up.hpi.informationsystems.adbms.csv.CSVParser.Implicits._
import de.up.hpi.informationsystems.adbms.relation.Relation
import de.up.hpi.informationsystems.sampleapp.dactors._

import scala.annotation.tailrec

object RelationBenchmark extends App {
  val dataDir = "/data_big"

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
    val relationName = f.getCanonicalPath.split(File.separatorChar).last.split('.').head
    nameValMapping(relationName).readFromFile(f)
  }

  def recursiveListFiles(d: File): List[File] = {
    val these = d.listFiles()
    these.filter(_.isFile).toList ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  // === Main ===
  val dataURL = getClass.getResource(dataDir)
  val fileList = recursiveListFiles(new File(dataURL.getPath))

  val relations = fileList.map(relationFromFile)

  while (true) { }

  println(relations.last)

}
