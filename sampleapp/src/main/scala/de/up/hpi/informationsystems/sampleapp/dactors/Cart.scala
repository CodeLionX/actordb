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
import scala.util.{Failure, Success}

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

  object CartInfo extends RelationDef {
    val cartId: ColumnDef[Int] = ColumnDef("c_id")
    val storeId: ColumnDef[Int] = ColumnDef("store_id")
    val sessionId: ColumnDef[Int] = ColumnDef("session_id")

    override val columns: Set[UntypedColumnDef] = Set(cartId, storeId, sessionId)
    override val name: String = "cart_info"
  }

  object CartPurchases extends RelationDef {
    val sectionId: ColumnDef[Int] = ColumnDef("sec_id")
    val sessionId: ColumnDef[Int] = ColumnDef("session_id")
    val inventoryId: ColumnDef[Int] = ColumnDef("i_id")
    val quantity: ColumnDef[Int] = ColumnDef("i_quantity")
    val fixedDiscount: ColumnDef[Double] = ColumnDef("i_fixed_disc")
    val minPrice: ColumnDef[Double] = ColumnDef("i_min_price")
    val price: ColumnDef[Double] = ColumnDef("i_price")

    override val columns: Set[UntypedColumnDef] =
      Set(sectionId, sessionId, inventoryId, quantity, fixedDiscount, minPrice, price)
    override val name: String = "cart_purchases"
  }

  private object AddItemsHelper {
    def apply(system: ActorSystem, backTo: ActorRef, askTimeout: Timeout): AddItemsHelper =
      new AddItemsHelper(system, backTo, askTimeout)

    case class Success(results: Seq[Record], replyTo: ActorRef)
    case class Failure(e: Throwable, replyTo: ActorRef)

    private[AddItemsHelper] case class PriceDiscountPartialResult(sectionId: Int, inventoryId: Int, price: Double, minPrice: Double, fixedDiscount: Double)
    private[AddItemsHelper] case class PricePartialResult(sectionId: Int, inventoryId: Int, price: Double, minPrice: Double)
    private[AddItemsHelper] case class DiscountsPartialResult(inventoryId: Int, fixedDiscount: Double) {
      def toInventoryDiscountTuple: (Int, Double) = (inventoryId, fixedDiscount)
    }
  }

  private class AddItemsHelper(system: ActorSystem, recipient: ActorRef, implicit val askTimeout: Timeout) {
    import de.up.hpi.informationsystems.sampleapp.dactors.Cart.AddItemsHelper.{DiscountsPartialResult, PriceDiscountPartialResult, PricePartialResult}
    import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
    import de.up.hpi.informationsystems.sampleapp.dactors.Cart.AddItems.Order
    import Dactor._

    def help(orders: Seq[Order], customerId: Int, replyTo: ActorRef): Unit = {
      val prices = Future.sequence(orders
        .groupBy(_.sectionId)
        .map(askStoreSectionForPrices))
        .map(_.flatten)

      val customerGroupId = askCustomerForGroupId(customerId)

      val inventoryIdsAndGroupId = for {
        prices <- prices
        customerGroupId <- customerGroupId
      } yield (prices.map(_.inventoryId), customerGroupId)

      val fixedDiscounts = inventoryIdsAndGroupId.flatMap{
        case (inventoryIds, groupId) => askGroupManagerForDiscount(inventoryIds.toSeq, groupId)
      }

      val pricesWithDiscounts = for {
        prices <- prices
        fixedDiscounts <- fixedDiscounts
      } yield joinPriceAndDisc(prices.toSeq, fixedDiscounts)

      val orderQuantities = orders.map{ order =>
        order.inventoryId -> order.quantity
      }.toMap

      val results = for {
        pricesWithDiscounts <- pricesWithDiscounts
      } yield joinPriceDiscountWithQuantities(pricesWithDiscounts, orderQuantities)

      results.map(records => AddItemsHelper.Success(records, replyTo)).pipeTo(recipient)
    }

    private def joinPriceDiscountWithQuantities(pricesWithDiscounts: Seq[PriceDiscountPartialResult], quantityOfInventoryId: Map[Int, Int]): Seq[Record] =
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

    private def joinPriceAndDisc(ppr: Seq[PricePartialResult], dpr: Seq[DiscountsPartialResult]): Seq[PriceDiscountPartialResult] = {
      val discountForInventoryId = dpr.map(_.toInventoryDiscountTuple).toMap

      ppr.map({
        case PricePartialResult(sectionId, inventoryId, price, minPrice) =>
          PriceDiscountPartialResult(sectionId, inventoryId, price, minPrice, discountForInventoryId.getOrElse(inventoryId, 0))
      })
    }

    private def askStoreSectionForPrices(inp: (Int, Seq[Order])): Future[Seq[PricePartialResult]] = {
      val (sectionId, orders) = inp
      val inventoryIds = orders.map(_.inventoryId)

      val answer = dactorSelection(system, classOf[StoreSection], sectionId) ? StoreSection.GetPrice.Request(inventoryIds)

      answer
        .mapTo[StoreSection.GetPrice.Success]                        // map to success response to fail fast
        .map( getPriceSuccess =>
          getPriceSuccess.result.map( r => {  // build result set
            PricePartialResult(
              sectionId = sectionId,
              inventoryId = r.get(ColumnDef[Int]("i_id")).get,
              price = r.get(ColumnDef[Double]("i_price")).get,
              minPrice = r.get(ColumnDef[Double]("i_min_price")).get
            )
          })
        )
    }

    private def askCustomerForGroupId(custId: Int): Future[Int] = {
      val answer = dactorSelection(system, classOf[Customer], custId) ? Customer.GetCustomerGroupId.Request()

      answer
        .mapTo[Customer.GetCustomerGroupId.Success]
        .map(_.result)
    }

    private def askGroupManagerForDiscount(inventoryIds: Seq[Int], groupId: Int): Future[Seq[DiscountsPartialResult]] = {
      val answer = dactorSelection(system, classOf[GroupManager], groupId) ? GroupManager.GetFixedDiscounts.Request(inventoryIds)

      answer
        .mapTo[GroupManager.GetFixedDiscounts.Success]
        .map(_.results.map( r =>
          DiscountsPartialResult(
            inventoryId = r.get(ColumnDef[Int]("i_id")).get,
            fixedDiscount = r.get(ColumnDef[Double]("fixed_disc")).get
          )
        ))
    }
  }
}

class Cart(id: Int) extends Dactor(id) {
  import Cart._

  private val timeout: Timeout = Timeout(2.seconds)

  override protected val relations: Map[RelationDef, MutableRelation] =
    Dactor.createAsRowRelations(Seq(CartInfo, CartPurchases))

  override def receive: Receive = {
    case AddItems.Request(orders, customerId) => AddItemsHelper(context.system, self, timeout).help(orders, customerId, sender())

    case AddItemsHelper.Success(records, replyTo) => relations(CartPurchases).insertAll(records) match {
      case Success(_) => replyTo ! AddItems.Success(currentSessionId)
      case Failure(e) => replyTo ! AddItems.Failure(e)
    }
    case AddItemsHelper.Failure(e, replyTo) => replyTo ! AddItems.Failure(e)

    case Checkout.Request(_) => sender() ! Checkout.Failure(new NotImplementedError)
  }

}

