package de.up.hpi.informationsystems.sampleapp.dactors

import akka.actor.Props
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition._

object Cart {

  def props(id: Int): Props = Props(new Cart(id))

  object AddItems {

    // orders: item_id, i_quantity
    case class Request(orders: Seq[Record], customerId: Int)
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

  override protected val relations: Map[String, MutableRelation] =
    Map("cart_info" -> CartInfo) ++ Map("cart_purchases" -> CartPurchases)

  override def receive: Receive = {
    case AddItems.Request(_, _) => sender() ! AddItems.Failure(new NotImplementedError)
    case Checkout.Request(_) => sender() ! Checkout.Failure(new NotImplementedError)
  }

}
