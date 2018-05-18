package de.up.hpi.informationsystems.sampleapp.dactors

import java.time.LocalDateTime

import akka.actor.Props
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition._

import scala.util.{Failure, Success, Try}

object StoreSection {

  def props(name: String): Props = Props(new StoreSection(name))

  object GetPrice {

    case class Request(inventoryIds: Seq[Int])

    case class Success(result: Seq[Record])
    // result

    case class Failure(e: Throwable)

  }

  object GetVariableDiscountUpdateInventory {

    case class Request(customerId: Int, cardId: Int, cartTime: LocalDateTime, orderItems: Seq[Record])
    // order items: i_id, i_quantity, i_min_price, i_price, i_fixed_disc

    case class Success(totals: Seq[Record])
    // totals: amount, fixed_disc, var_disc

    case class Failure(e: Throwable)
  }

}

class StoreSection(name: String) extends Dactor(name) {
  import StoreSection._

  object Inventory extends RowRelation {
    val inventoryId: ColumnDef[Int] = ColumnDef("i_id")
    val price: ColumnDef[Double] = ColumnDef("i_price")
    val minPrice: ColumnDef[Double] = ColumnDef("i_min_price")
    val quantity: ColumnDef[Long] = ColumnDef("i_quantity")
    val varDisc: ColumnDef[Double] = ColumnDef("i_var_disc")

    override val columns: Set[UntypedColumnDef] = Set(inventoryId, price, minPrice, quantity, varDisc)
  }

  object PurchaseHistory extends RowRelation {
    val inventoryId: ColumnDef[Int] = ColumnDef("i_id")
    val time: ColumnDef[LocalDateTime] = ColumnDef("time")
    val quantity: ColumnDef[Long] = ColumnDef("i_quantity")
    val cartId: ColumnDef[Int] = ColumnDef("c_id")

    override val columns: Set[UntypedColumnDef] = Set(inventoryId, time, quantity, cartId)
  }

  override protected val relations: Map[String, Relation] =
    Map("inventory" -> Inventory) ++
    Map("purchase_history" -> PurchaseHistory)


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
    Inventory
      .project(resultSchema)
      .where[Int](Inventory.inventoryId -> { id => inventoryIds.contains(id) })
      .records
  }

}