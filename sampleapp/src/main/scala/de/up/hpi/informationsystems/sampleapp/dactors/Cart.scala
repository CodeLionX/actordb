package de.up.hpi.informationsystems.sampleapp.dactors

import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
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

  private var currentSessionId = 0

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

  def addItems(orders: Seq[AddItems.Order], customerId: Int): Future[Try[Seq[Record]]] = {
    // ask for prices of orders
    // i_id, i_price, i_min_price
    val prices = Future.sequence(orders
      .groupBy(order => order.sectionId)
      .map(askStoreSectionForPrices))

    // c_g_id
    val customer_group_id = askCustomerForGroupId(customerId)

    // collect i_ids, group_id to query for fixed_disc
    val i_ids_and_group_id = for {
      prices <- prices
      c_g_id <- customer_group_id
    } yield (prices.flatten.map(ppr => ppr.i_id), c_g_id)

    // i_id, fixed_disc
    val fixed_disc = i_ids_and_group_id.flatMap(tuple => {
      val (i_ids, group_id) = tuple
      askGroupManagerForDiscount(i_ids.toSeq, group_id)
    })

    // join prices with their discounts:
    // sec_id, i_id, sess_id (new), i_quantity, i_fixed_disc, i_min_price, i_price
    val prices_with_discounts = for {
      prices <- prices
      fixed_disc <- fixed_disc
    } yield joinPriceAndDisc(prices.flatten.toSeq, fixed_disc)

    // join with quantities, add sec_id, sess_id,
    val result = for {
      prices_with_discounts <- prices_with_discounts
    } yield joinPriceDiscountWithQuantities(prices_with_discounts, (orders map {order => order.inventoryId -> order.quantity}).toMap)

    result.map(records => {
      CartPurchases.insertAll(records)
    })
  }

  private def joinPriceDiscountWithQuantities(prices_with_discounts: Seq[Cart.this.PriceDiscountPartialResult], quantityOfInventoryId: Map[Int, Int]) =
    prices_with_discounts map {
      case PriceDiscountPartialResult(s_id, i_id, i_price, i_min_price, i_fixed_disc) =>
        CartPurchases.newRecord
          .withCellContent(CartPurchases.sectionId)(s_id)
          .withCellContent(CartPurchases.sessionId)(currentSessionId)
          .withCellContent(CartPurchases.inventoryId)(i_id)
          .withCellContent(CartPurchases.quantity)(quantityOfInventoryId.getOrElse(i_id, 0))
          .withCellContent(CartPurchases.price)(i_price)
          .withCellContent(CartPurchases.minPrice)(i_min_price)
          .withCellContent(CartPurchases.fixedDiscount)(i_fixed_disc)
          .build()
    }

  case class PriceDiscountPartialResult(s_id: Int, i_id: Int, i_price: Double, i_min_price: Double, i_fixed_disc: Double)
  private def joinPriceAndDisc(ppr: Seq[Cart.this.PricePartialResult], dpr: Seq[Cart.this.DiscountsPartialResult]) = {
    val discount_for_i_id = (dpr map {
      a => a.i_id -> a.i_fixed_disc
    }).toMap

    ppr.map({
      case PricePartialResult(s_id, i_id, i_price, i_min_price) =>
        PriceDiscountPartialResult(s_id, i_id, i_price, i_min_price, discount_for_i_id.getOrElse(i_id, 0))
    })
  }

  case class PricePartialResult(s_id: Int, i_id: Int, i_price: Double, i_min_price: Double)
  private def askStoreSectionForPrices(inp: (Int, Seq[AddItems.Order])): Future[Seq[PricePartialResult]] = {
    val (sectionId, orders) = inp
    val inventoryIds = orders.map(order => order.inventoryId)

    implicit val timeout = Timeout(2 seconds)

    val answer = dactorSelection(classOf[StoreSection], sectionId) ? StoreSection.GetPrice.Request(inventoryIds)

    val result = answer
      .mapTo[StoreSection.GetPrice.Success]                        // map to success response to fail fast
      .map((getPriceSuccess) => getPriceSuccess.result.map(r => {  // build result set
      PricePartialResult(
        s_id = sectionId,
        i_id = r.get(ColumnDef[Int]("i_id")).get,
        i_price = r.get(ColumnDef[Double]("i_price")).get,
        i_min_price = r.get(ColumnDef[Double]("i_min_price")).get
      )
    }))
    result
  }

  private def askCustomerForGroupId(custId: Int): Future[Int] = {
    implicit val timeout = Timeout(2 seconds)
    val answer = dactorSelection(classOf[Customer], custId) ? Customer.GetCustomerGroupId.Request()
    answer
      .mapTo[Customer.GetCustomerGroupId.Success]
      .map(getGroupIdSuccess => getGroupIdSuccess.result)
  }

  case class DiscountsPartialResult(i_id: Int, i_fixed_disc: Double)
  private def askGroupManagerForDiscount(inventoryIds: Seq[Int], groupId: Int): Future[Seq[DiscountsPartialResult]] = {
    implicit val timeout = Timeout(2 seconds)
    val answer = dactorSelection(classOf[GroupManager], groupId) ? GroupManager.GetFixedDiscounts.Request(inventoryIds)
    val result = answer
      .mapTo[GroupManager.GetFixedDiscounts.Success]
      .map(getDiscountsSuccess => getDiscountsSuccess.results.map(r =>
        DiscountsPartialResult(
          i_id = r.get(ColumnDef[Int]("i_id")).get,
          i_fixed_disc = r.get(ColumnDef[Double]("fixed_disc")).get
        )
      ))
    result
  }

}
