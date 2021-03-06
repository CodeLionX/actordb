package de.up.hpi.informationsystems.sampleapp.dactors

import java.time.{ZoneOffset, ZonedDateTime}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._
import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.adbms.protocols.{DefaultMessageHandling, RequestResponseProtocol}
import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation._
import de.up.hpi.informationsystems.sampleapp.DataInitializer
import de.up.hpi.informationsystems.sampleapp.dactors.Cart.CartBase

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Cart {

  def props(id: Int): Props = Props(new Cart(id))

  object AddItems {
    case class Order(inventoryId: Int, sectionId: Int, quantity: Int)

    sealed trait AddItems extends RequestResponseProtocol.Message
    // orders: item_id, i_quantity
    case class Request(orders: Seq[Order], customerId: Int) extends RequestResponseProtocol.Request[AddItems]
    case class Success(result: Relation) extends RequestResponseProtocol.Success[AddItems]
    case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[AddItems]
  }

  object Checkout {
    sealed trait Checkout extends RequestResponseProtocol.Message
    case class Request(sessionId: Int) extends RequestResponseProtocol.Request[Checkout]
    case class Success(result: Relation) extends RequestResponseProtocol.Success[Checkout]
    case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[Checkout]
  }

  object CartInfo extends RelationDef {
    val customerId: ColumnDef[Int] = ColumnDef[Int]("c_id")
    val storeId: ColumnDef[Int] = ColumnDef[Int]("store_id")
    val sessionId: ColumnDef[Int] = ColumnDef[Int]("session_id")

    override val columns: Set[UntypedColumnDef] = Set(customerId, storeId, sessionId)
    override val name: String = "cart_info"
  }

  object CartPurchases extends RelationDef {
    val sectionId: ColumnDef[Int] = ColumnDef[Int]("sec_id")
    val sessionId: ColumnDef[Int] = ColumnDef[Int]("session_id")
    val inventoryId: ColumnDef[Int] = ColumnDef[Int]("i_id")
    val quantity: ColumnDef[Int] = ColumnDef[Int]("i_quantity")
    val fixedDiscount: ColumnDef[Double] = ColumnDef[Double]("i_fixed_disc")
    val minPrice: ColumnDef[Double] = ColumnDef[Double]("i_min_price")
    val price: ColumnDef[Double] = ColumnDef[Double]("i_price")

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

      val fixedDiscount: FutureRelation = groupId.flatTransform( gid => {
        val id = gid.records.get.head.get(Customer.CustomerInfo.custGroupId).get
        val fixedDiscountRequest = Map(id -> GroupManager.GetFixedDiscounts.Request(orders.map(_.inventoryId)))
        Dactor.askDactor(system, classOf[GroupManager], fixedDiscountRequest)
      })
      // FutureRelation: i_id, fixed_disc

      val priceDisc: FutureRelation = priceList.innerJoin(fixedDiscount, (priceRec, discRec) =>
        priceRec.get(CartPurchases.inventoryId) == discRec.get(CartPurchases.inventoryId)
      ).asInstanceOf[FutureRelation]
      // FutureRelation: i_id, i_price, i_min_price, fixed_disc

      val orderRecordBuilder = Record(Set(CartPurchases.inventoryId, CartPurchases.sectionId, CartPurchases.quantity))
      val orderRelation: Relation = Relation(orders.map(order => orderRecordBuilder(
        CartPurchases.inventoryId ~> order.inventoryId &
        CartPurchases.sectionId ~> order.sectionId &
        CartPurchases.quantity ~> order.quantity
      ).build()))
      val priceDiscOrder: FutureRelation = priceDisc.innerJoin(orderRelation, (priceRec, orderRec) =>
        priceRec.get(CartPurchases.inventoryId) == orderRec.get(CartPurchases.inventoryId)
      ).asInstanceOf[FutureRelation]
      // FutureRelation: i_id, i_price, i_min_price, fixed_disc, sec_id, i_quantity

      val result: FutureRelation = priceDiscOrder.transform( relation => Relation(
        relation.records.get
          .map( (rec: Record) => rec + (CartPurchases.sessionId -> currentSessionId))
          .map( (rec: Record) => CartPurchases.newRecord(
            CartPurchases.sectionId ~> rec.get(CartPurchases.sectionId).get &
            CartPurchases.sessionId ~> rec.get(CartPurchases.sessionId).get &
            CartPurchases.quantity ~> rec.get(CartPurchases.quantity).get &
            CartPurchases.inventoryId ~> rec.get(StoreSection.Inventory.inventoryId).get &
            CartPurchases.fixedDiscount ~> rec.get(GroupManager.Discounts.fixedDisc).get &
            CartPurchases.minPrice ~> rec.get(StoreSection.Inventory.minPrice).get &
            CartPurchases.price ~> rec.get(StoreSection.Inventory.price).get
          ).build())
      ))
      // FutureRelation: i_id, i_price, i_min_price, i_fixed_disc, sec_id, i_quantity, session_id

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

  class CartBase(id: Int) extends Dactor(id) {

    private val timeout: Timeout = Timeout(2.seconds)
    private val helper: CartHelper = CartHelper(context.system, self, timeout)

    override protected val relations: Map[RelationDef, MutableRelation] =
      Map(CartInfo -> SingleRowRelation(CartInfo)) ++
      Map(CartPurchases -> RowRelation(CartPurchases))

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
          case Success(_) => {
            val result: Relation = Relation(Seq(Record(Set(CartInfo.sessionId))(CartInfo.sessionId ~> newSessionId).build()))
            replyTo ! AddItems.Success(result)
          }
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
            Map((CartPurchases.sessionId: UntypedColumnDef) -> { sesId: Any => sesId.asInstanceOf[Int] == sessionId}) ++
              Map((CartPurchases.sectionId: UntypedColumnDef) -> { secId: Any => sections.contains(secId.asInstanceOf[Int]) })
          ).project(Set(
            CartPurchases.inventoryId, CartPurchases.quantity, CartPurchases.price,
            CartPurchases.fixedDiscount, CartPurchases.minPrice
        ))

        helper.handleCheckout(customerId, storeId, sections, ZonedDateTime.now(ZoneOffset.UTC), cartItems, sender())
      }

      case CartHelper.HandledCheckout(amount, replyTo) =>
        replyTo ! Checkout.Success(Relation(Seq(Record(Set(ColumnDef[Double]("amount")))(ColumnDef[Double]("amount") ~> amount).build())))
    }
  }
}

class Cart(id: Int)
  extends CartBase(id)
    with DataInitializer
    with DefaultMessageHandling