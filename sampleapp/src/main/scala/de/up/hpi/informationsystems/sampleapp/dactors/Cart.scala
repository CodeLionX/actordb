package de.up.hpi.informationsystems.sampleapp.dactors

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.Record.RecordBuilder
import de.up.hpi.informationsystems.adbms.definition._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object Cart {

  def props(id: Int): Props = Props(new Cart(id))

  private var currentSessionId = 0

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

  private object CartInfo extends RowRelation {
    val cartId: ColumnDef[Int] = ColumnDef("c_id")
    val storeId: ColumnDef[Int] = ColumnDef("store_id")
    val sessionId: ColumnDef[Int] = ColumnDef("session_id")

    override val columns: Set[UntypedColumnDef] = Set(cartId, storeId, sessionId)
  }

  private object CartPurchases extends RowRelation {
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

  private object AddItemsHelper {
    def apply(system: ActorSystem, backTo: ActorRef): AddItemsHelper = new AddItemsHelper(system, backTo)

    case class Response(results: Seq[Record])

    private case class PriceDiscountPartialResult(sectionId: Int, inventoryId: Int, price: Double, minPrice: Double, fixedDiscount: Double)
    private case class PricePartialResult(sectionId: Int, inventoryId: Int, price: Double, minPrice: Double)
    private case class DiscountsPartialResult(inventoryId: Int, fixedDiscount: Double)
  }

  private class AddItemsHelper(system: ActorSystem, recipient: ActorRef) {
    import de.up.hpi.informationsystems.sampleapp.dactors.Cart.AddItemsHelper.{DiscountsPartialResult, PriceDiscountPartialResult, PricePartialResult}
    import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
    import de.up.hpi.informationsystems.sampleapp.dactors.Cart.AddItems.Order
    import Dactor._

    def addItems(orders: Seq[Order], customerId: Int) = {
      val prices = Future.sequence(orders
        .groupBy(order => order.sectionId)
        .map(askStoreSectionForPrices))

      val customerGroupId = askCustomerForGroupId(customerId)

      val inventoryIdsAndGroupId = for {
        prices <- prices
        customerGroupId <- customerGroupId
      } yield (prices.flatten.map(ppr => ppr.inventoryId), customerGroupId)

      val fixedDiscounts = inventoryIdsAndGroupId.flatMap(tuple => {
        val (i_ids, group_id) = tuple
        askGroupManagerForDiscount(i_ids.toSeq, group_id)
      })

      val pricesWithDiscounts = for {
        prices <- prices
        fixedDiscounts <- fixedDiscounts
      } yield joinPriceAndDisc(prices.flatten.toSeq, fixedDiscounts)

      val results = for {
        pricesWithDiscounts <- pricesWithDiscounts
      } yield joinPriceDiscountWithQuantities(pricesWithDiscounts, (orders map {order => order.inventoryId -> order.quantity}).toMap)

      results.map(records => AddItemsHelper.Response(records)).pipeTo(recipient)
    }

    private def joinPriceDiscountWithQuantities(pricesWithDiscounts: Seq[PriceDiscountPartialResult], quantityOfInventoryId: Map[Int, Int]) =
      pricesWithDiscounts map {
        case PriceDiscountPartialResult(sectionId, inventoryId, price, minPrice, fixedDiscount) =>
          CartPurchases.newRecord(
            CartPurchases.sessionId ~> currentSessionId &
            CartPurchases.sectionId ~> sectionId &
            CartPurchases.inventoryId ~> inventoryId &
            CartPurchases.quantity ~> quantityOfInventoryId.getOrElse(inventoryId, 0) &
            CartPurchases.price ~> price &
            CartPurchases.minPrice ~> minPrice &
            CartPurchases.fixedDiscount ~> fixedDiscount
          ).build()
      }

    private def joinPriceAndDisc(ppr: Seq[PricePartialResult], dpr: Seq[DiscountsPartialResult]) = {
      val discountForInventoryId = (dpr map {
        a => a.inventoryId -> a.fixedDiscount
      }).toMap

      ppr.map({
        case PricePartialResult(sectionId, inventoryId, price, minPrice) =>
          PriceDiscountPartialResult(sectionId, inventoryId, price, minPrice, discountForInventoryId.getOrElse(inventoryId, 0))
      })
    }

    private def askStoreSectionForPrices(inp: (Int, Seq[Order])): Future[Seq[PricePartialResult]] = {
      val (sectionId, orders) = inp
      val inventoryIds = orders.map(order => order.inventoryId)

      implicit val timeout: Timeout = Timeout(2.seconds)

      val answer = dactorSelection(system, classOf[StoreSection], sectionId) ? StoreSection.GetPrice.Request(inventoryIds)

      val result = answer
        .mapTo[StoreSection.GetPrice.Success]                        // map to success response to fail fast
        .map((getPriceSuccess) => getPriceSuccess.result.map(r => {  // build result set
        PricePartialResult(
          sectionId = sectionId,
          inventoryId = r.get(ColumnDef[Int]("i_id")).get,
          price = r.get(ColumnDef[Double]("i_price")).get,
          minPrice = r.get(ColumnDef[Double]("i_min_price")).get
        )
      }))
      result
    }

    private def askCustomerForGroupId(custId: Int): Future[Int] = {
      implicit val timeout: Timeout = Timeout(2.seconds)
      val answer = dactorSelection(system, classOf[Customer], custId) ? Customer.GetCustomerGroupId.Request()
      answer
        .mapTo[Customer.GetCustomerGroupId.Success]
        .map(getGroupIdSuccess => getGroupIdSuccess.result)
    }

    private def askGroupManagerForDiscount(inventoryIds: Seq[Int], groupId: Int): Future[Seq[DiscountsPartialResult]] = {
      implicit val timeout: Timeout = Timeout(2.seconds)
      val answer = dactorSelection(system, classOf[GroupManager], groupId) ? GroupManager.GetFixedDiscounts.Request(inventoryIds)
      val result = answer
        .mapTo[GroupManager.GetFixedDiscounts.Success]
        .map(getDiscountsSuccess => getDiscountsSuccess.results.map(r =>
          DiscountsPartialResult(
            inventoryId = r.get(ColumnDef[Int]("i_id")).get,
            fixedDiscount = r.get(ColumnDef[Double]("fixed_disc")).get
          )
        ))
      result
    }
  }
}

class Cart(id: Int) extends Dactor(id) {
  import Cart._

  override protected val relations: Map[String, Relation] =
    Map("cart_info" -> CartInfo) ++ Map("cart_purchases" -> CartPurchases)

  override def receive: Receive = {
    case AddItems.Request(orders, customerId) => AddItemsHelper(context.system, self).addItems(orders, customerId)
    case AddItemsHelper.Response(result) => CartPurchases.insertAll(result)
    case Checkout.Request(_) => sender() ! Checkout.Failure(new NotImplementedError)
  }

  def newCartPurchaseRecord: RecordBuilder = CartPurchases.newRecord

}

