package de.up.hpi.informationsystems.sampleapp.dactors

import java.time.{ZoneOffset, ZonedDateTime}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.definition._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

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

  object CartInfo extends RelationDef {
    val customerId: ColumnDef[Int] = ColumnDef("c_id")
    val storeId: ColumnDef[Int] = ColumnDef("store_id")
    val sessionId: ColumnDef[Int] = ColumnDef("session_id")

    override val columns: Set[UntypedColumnDef] = Set(customerId, storeId, sessionId)
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

  private object CartHelper {
    def apply(system: ActorSystem, backTo: ActorRef, askTimeout: Timeout): CartHelper =
      new CartHelper(system, backTo, askTimeout)

    case class HandledAddItems(results: Seq[Record], newSessionId: Int, replyTo: ActorRef)
    case class HandledCheckout(amount: Long, replyTo: ActorRef)
    case class Failure(e: Throwable, replyTo: ActorRef)

  }

  private class CartHelper(system: ActorSystem, recipient: ActorRef, implicit val askTimeout: Timeout) {
    import de.up.hpi.informationsystems.sampleapp.dactors.Cart.AddItems.Order

    def handleAddItems(orders: Seq[Order], customerId: Int, currentSessionId: Int, replyTo: ActorRef): Unit = {

      val priceRequests = orders
        .groupBy(_.sectionId)
        .map{ case (sectionId, sectionOrders) =>
          sectionId -> StoreSection.GetPrice.Request(sectionOrders.map(_.inventoryId))
        }
      val priceList: FutureRelation = Dactor.askDactor(system, classOf[StoreSection], priceRequests)
      // FutureRelation: i_id, i_price, i_min_price

      val groupIdRequest = Map(customerId -> Customer.GetCustomerGroupId.Request())
      val groupId: FutureRelation = Dactor
        .askDactor(system, classOf[Customer], groupIdRequest)

      val fixedDiscount: FutureRelation = groupId.flatTransform( groupId => {
        val id = groupId.records.get.head.get(Customer.CustomerInfo.custGroupId).get
        val fixedDiscountRequest = Map(id -> GroupManager.GetFixedDiscounts.Request(orders.map(_.inventoryId)))
        Dactor.askDactor(system, classOf[GroupManager], fixedDiscountRequest)
      })
      // FutureRelation: i_id, fixed_disc

      val priceDisc: FutureRelation = priceList.innerJoin(fixedDiscount, (priceRec, discRec) =>
        priceRec.get(CartPurchases.inventoryId) == discRec.get(CartPurchases.inventoryId)
      )
      // FutureRelation: i_id, i_price, i_min_price, fixed_disc

      val orderRecordBuilder = Record(Set(CartPurchases.inventoryId, CartPurchases.sectionId, CartPurchases.quantity))
      val orderRelation = FutureRelation.fromRecordSeq(Future{orders.map(order => orderRecordBuilder(
        CartPurchases.inventoryId ~> order.inventoryId &
          CartPurchases.sectionId ~> order.sectionId &
          CartPurchases.quantity ~> order.quantity
      ).build())})
      val priceDiscOrder: FutureRelation = priceDisc.innerJoin(orderRelation, (priceRec, orderRec) =>
        priceRec.get(CartPurchases.inventoryId) == orderRec.get(CartPurchases.inventoryId)
      )
      // FutureRelation: i_id, i_price, i_min_price, fixed_disc, sec_id, i_quantity

      val result = FutureRelation.fromRecordSeq(priceDiscOrder.future.map(_.records.get.map( (rec: Record) => rec + (CartPurchases.sessionId -> currentSessionId))))
      // FutureRelation: i_id, i_price, i_min_price, fixed_disc, sec_id, i_quantity, session_id

      result.pipeAsMessageTo(relation => CartHelper.HandledAddItems(relation.records.get, currentSessionId, replyTo), recipient)
    }

    def handleCheckout(customerId: Int, storeId: Int, sectionIds: Seq[Int], time: ZonedDateTime, allCartItems: Relation, replyTo: ActorRef): Unit = {
      // request variable discounts and get sums of storeSections
      val purchaseRequests = sectionIds.map( sectionId => {
        val cartItems: Relation = allCartItems.where[Int](CartPurchases.sectionId -> { _ == sectionId })

        sectionId -> StoreSection.GetVariableDiscountUpdateInventory.Request(
          customerId,
          time,
          cartItems
        )
      }).toMap
      val variableDiscounts: FutureRelation = Dactor.askDactor(system, classOf[StoreSection], purchaseRequests)

      // sum all discounts up
      val amountCol = StoreSection.GetVariableDiscountUpdateInventory.amountCol
      val fixedDiscCol = StoreSection.GetVariableDiscountUpdateInventory.fixedDiscCol
      val varDiscCol = StoreSection.GetVariableDiscountUpdateInventory.varDiscCol

      val variableDiscountSum = variableDiscounts.transform(relation =>
        Relation(Seq(
          relation.records.get.reduce( (rec1, rec2) =>
            Record(rec1.columns)(
              amountCol ~> (rec1.get(amountCol).get + rec2.get(amountCol).get) &
              fixedDiscCol ~> (rec1.get(fixedDiscCol).get + rec2.get(fixedDiscCol).get) &
              varDiscCol ~> (rec1.get(varDiscCol).get + rec2.get(varDiscCol).get)
            ).build()
          )
        ))
      )

      // notify customer
      val result = variableDiscountSum.transform( relation => {
        val record = relation.records.get.head
        val msg = Customer.AddStoreVisit.Request(
          storeId,
          time,
          record.get(amountCol).get,
          record.get(fixedDiscCol).get,
          record.get(varDiscCol).get
        )
        Dactor.dactorSelection(system, classOf[Customer], customerId) ! msg
        relation
      })

      // wrap into message and send back to actor
      result.pipeAsMessageTo(relation =>
        CartHelper.HandledCheckout(
          relation.records.get.head.get(amountCol).get,
          replyTo
        ), recipient)
    }
  }
}

class Cart(id: Int) extends Dactor(id) {
  import Cart._

  private val timeout: Timeout = Timeout(2.seconds)
  private val helper: CartHelper = CartHelper(context.system, self, timeout)

  override protected val relations: Map[RelationDef, MutableRelation] =
    Dactor.createAsRowRelations(Seq(CartInfo, CartPurchases))

  private var currentSessionId = 0

  override def receive: Receive = {
    case AddItems.Request(orders, customerId) => {
      currentSessionId += 1
      relations(CartInfo)
        .update(CartInfo.sessionId ~> currentSessionId)
        .where[Int](CartInfo.customerId -> { _ == customerId })
      helper.handleAddItems(orders, customerId, currentSessionId, sender())
    }

    case CartHelper.HandledAddItems(records, newSessionId, replyTo) =>
      relations(CartPurchases).insertAll(records) match {
        case Success(_) => replyTo ! AddItems.Success(newSessionId)
        case Failure(e) => replyTo ! AddItems.Failure(e)
      }

    case Checkout.Request(sessionId) => {
      // TODO: check there is only one entry
      val customerId: Int = relations(CartInfo)
        .records.get
        .map(_.get(CartInfo.customerId).get).head
      val storeId: Int = relations(CartInfo)
        .records.get
        .map(_.get(CartInfo.storeId).get).head

      val sections: Seq[Int] = relations(CartPurchases)
        .where[Int](CartPurchases.sessionId -> { _ == sessionId })
        .project(Set(CartPurchases.sectionId))
        .records.get.map(_.get(CartPurchases.sectionId).get).distinct

      val cartItems: Relation = relations(CartPurchases)
        .whereAll(
          Map(CartPurchases.sessionId.untyped -> { sesId: Any => sesId.asInstanceOf[Int] == sessionId}) ++
            Map(CartPurchases.sectionId.untyped -> { secId: Any => sections.contains(secId.asInstanceOf[Int]) })
        ).project(Set(
        CartPurchases.inventoryId, CartPurchases.quantity, CartPurchases.price,
        CartPurchases.fixedDiscount, CartPurchases.minPrice
      ))

      helper.handleCheckout(customerId, storeId, sections, ZonedDateTime.now(ZoneOffset.UTC), cartItems, sender())
    }

    case CartHelper.HandledCheckout(amount, replyTo) =>
      replyTo ! Checkout.Success(amount)
  }
}
