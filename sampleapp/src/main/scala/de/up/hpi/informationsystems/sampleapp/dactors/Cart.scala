package de.up.hpi.informationsystems.sampleapp.dactors

import akka.actor.Props
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition._

import scala.util.{Failure, Success, Try}

object Cart {

  def props(id: Int): Props = Props(new Cart(id))

  object AddItems {

    case class Order(inventoryId: Int, sectionId: Int, quantity: Int)

    // orders: item_id, i_quantity
    case class Request(orders: Seq[Order], customerId: Int)
    case class Success(sessionId: Int)
    case class Failure(e: Throwable)

  }

  object Checkout {

    case class Request(sessionId: Int)
    case class Success(amount: Double)
    case class Failure(e: Throwable)

  }
}

class Cart(id: Int) extends Dactor(id) {
  import Cart._

  object CartInfo extends RowRelation {
    val cartId: ColumnDef[Int] = ColumnDef("c_id")
    val storeId: ColumnDef[Int] = ColumnDef("store_id")
    val sessionId: ColumnDef[Int] = ColumnDef("session_id")

    override val columns: Set[UntypedColumnDef] = Set(cartId, storeId, sessionId)
  }

  object CartPurchases extends RowRelation {
    val sectionId: ColumnDef[Int] = ColumnDef("sec_id")
    val sessionId: ColumnDef[Int] = ColumnDef("session_id")
    val inventoryId: ColumnDef[Int] = ColumnDef("i_id")
    val quantity: ColumnDef[Int] = ColumnDef("i_quantity")
    val fixedDiscount: ColumnDef[Double] = ColumnDef("i_fixed_disc")
    val minPrice: ColumnDef[Double] = ColumnDef("i_min_price")
    val price: ColumnDef[Double] = ColumnDef("i_price")

    override val columns: Set[UntypedColumnDef] =
      Set(sectionId, sessionId, inventoryId, quantity, fixedDiscount, minPrice, price)
  }

  override protected val relations: Map[String, Relation] =
    Map("cart_info" -> CartInfo) ++ Map("cart_purchases" -> CartPurchases)

  override def receive: Receive = {
    case AddItems.Request(orders, customerId) =>
      addItems(orders, customerId) match {
        case Success(sessionId) => sender() ! AddItems.Success(sessionId)
        case Failure(e) => sender() ! AddItems.Failure(e)
      }

    case Checkout.Request(_) => sender() ! Checkout.Failure(new NotImplementedError)
  }

  def addItems(orders: Seq[AddItems.Order], i: Int): Try[Int] = {
    for ((sectionId, orders) <- orders.groupBy({ case AddItems.Order(_, sec_id, _) => sec_id})) {
      val storeSection = dactorSelection(classOf[StoreSection], sectionId)
      storeSection ! StoreSection.GetPrice.Request(orders.map({ case AddItems.Order(i_id, _, _) => i_id}))
    }
  }

}
