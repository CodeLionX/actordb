package de.up.hpi.informationsystems.sampleapp.dactors

import java.time.LocalDateTime

import akka.actor.Props
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition._

import scala.util.{Failure, Success, Try}

object StoreSection {

  def props(id: Int): Props = Props(new StoreSection(id))

  object GetPrice {

    case class Request(inventoryIds: Seq[Int])
    // result: i_price, i_min_price
    case class Success(result: Seq[Record])
    case class Failure(e: Throwable)

  }

  object GetVariableDiscountUpdateInventory {

    // order items: i_id, i_quantity, i_min_price, i_price, i_fixed_disc
    case class Request(customerId: Int, cardId: Int, cartTime: LocalDateTime, orderItems: Seq[Record])
    // totals: amount, fixed_disc, var_disc
    case class Success(totals: Seq[Record])
    case class Failure(e: Throwable)

  }

  object Inventory extends RelationDef {
    val inventoryId: ColumnDef[Int] = ColumnDef("i_id")
    val price: ColumnDef[Double] = ColumnDef("i_price")
    val minPrice: ColumnDef[Double] = ColumnDef("i_min_price")
    val quantity: ColumnDef[Long] = ColumnDef("i_quantity")
    val varDisc: ColumnDef[Double] = ColumnDef("i_var_disc")

    override val columns: Set[UntypedColumnDef] = Set(inventoryId, price, minPrice, quantity, varDisc)
    override val name: String = "inventory"
  }

  object PurchaseHistory extends RelationDef {
    val inventoryId: ColumnDef[Int] = ColumnDef("i_id")
    val time: ColumnDef[LocalDateTime] = ColumnDef("time")
    val quantity: ColumnDef[Long] = ColumnDef("i_quantity")
    val cartId: ColumnDef[Int] = ColumnDef("c_id")

    override val columns: Set[UntypedColumnDef] = Set(inventoryId, time, quantity, cartId)
    override val name: String = "purchase_history"
  }
}

class StoreSection(id: Int) extends Dactor(id) {
  import StoreSection._

  val inventory = RowRelation(Inventory)
  val purchaseHistory = RowRelation(PurchaseHistory)

  override protected val relations: Map[String, MutableRelation] =
    Map(Inventory.name -> inventory) ++
    Map(PurchaseHistory.name -> purchaseHistory)

  override def receive: Receive = {
    case GetPrice.Request(inventoryIds) =>
      getPrice(inventoryIds) match {
        case Success(result) => sender() ! GetPrice.Success(result)
        case Failure(e) => sender() ! GetPrice.Failure(e)
      }

    case GetVariableDiscountUpdateInventory.Request =>
      sender() ! GetVariableDiscountUpdateInventory.Failure(new NotImplementedError)
  }

  def getPrice(inventoryIds: Seq[Int]): Try[Seq[Record]] = {
    val resultSchema = Set(Inventory.price, Inventory.minPrice)
    inventory
      .project(resultSchema)
      .where[Int](Inventory.inventoryId -> { id => inventoryIds.contains(id) })
      .records
  }

}