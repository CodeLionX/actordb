package de.up.hpi.informationsystems.sampleapp.dactors

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
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

  private object AddItemsHelper {
    def apply(system: ActorSystem, backTo: ActorRef, askTimeout: Timeout): AddItemsHelper =
      new AddItemsHelper(system, backTo, askTimeout)

    case class Success(results: Seq[Record], customerId: Int, newSessionId: Int, replyTo: ActorRef)
    case class Failure(e: Throwable, replyTo: ActorRef)

    private[AddItemsHelper] case class PriceDiscountPartialResult(sectionId: Int, inventoryId: Int, price: Double, minPrice: Double, fixedDiscount: Double)
    private[AddItemsHelper] object PricePartialResult {
      def forSection(id: Int): Record => PricePartialResult = r =>
        PricePartialResult(
          sectionId = id,
          inventoryId = r.get(ColumnDef[Int]("i_id")).get,
          price = r.get(ColumnDef[Double]("i_price")).get,
          minPrice = r.get(ColumnDef[Double]("i_min_price")).get
        )
    }
    private[AddItemsHelper] case class PricePartialResult(sectionId: Int, inventoryId: Int, price: Double, minPrice: Double)
    private[AddItemsHelper] case class DiscountsPartialResult(inventoryId: Int, fixedDiscount: Double) {
      def toInventoryDiscountTuple: (Int, Double) = (inventoryId, fixedDiscount)
    }
  }

  private class AddItemsHelper(system: ActorSystem, recipient: ActorRef, implicit val askTimeout: Timeout) {
    import de.up.hpi.informationsystems.sampleapp.dactors.Cart.AddItems.Order

    def help(orders: Seq[Order], customerId: Int, currentSessionId: Int, replyTo: ActorRef): Unit = {
      /*
      orders_by_store_section = extract_arrange(orders);
       */
      val priceRequests = orders
        .groupBy(_.sectionId)
        .map{ case (sectionId, sectionOrders) =>
          sectionId -> StoreSection.GetPrice.Request(sectionOrders.map(_.inventoryId))
        }

      /*
      map<int, future> results;
      for (section_order: orders_by_store_section) {
        future res := actor<Store_Section>[section_order.sec_id].get_price(section_order.item_ids)
        results.add(section_order.sec_id, res)
      }
       */
      val priceList: FutureRelation = Dactor.askDactor[StoreSection.GetPrice.Success](system, classOf[StoreSection], priceRequests)
      // FutureRelation: i_id, i_price, i_min_price
      // FIXME: mapping from section (actorId = sectionId) to content (i_id, i_price, i_min_price) gets lost

      /*
      SELECT c_g_id INTO v_c_g_id
        FROM actor<Customer>.get_customer_info()
        WHERE name = o_c_id;
       */
      val groupIdRequest = Map(customerId -> Customer.GetCustomerGroupId.Request())
      val groupId: FutureRelation = Dactor
        .askDactor[Customer.GetCustomerGroupId.Success](system, classOf[Customer], groupIdRequest)
        // full:
        //        .future.map {
        //          case Success(records) =>
        //            if(records.size == 1)
        //              records.head.get(ColumnDef[Int]("c_g_id")) match {
        //                case Some(id) => id
        //                case None => throw RuntimeException
        //              }
        //          case Failure(e) => throw e
        //        }
        // short:
        //.future.map(_.get.head.get(Customer.CustomerInfo.custGroupId).get)

      /*
      ordered_item_ids := extract_ids(orders);
    import de.up.hpi.informationsystems.sampleapp.dactors.Cart.AddItemsHelper.{DiscountsPartialResult, PriceDiscountPartialResult, PricePartialResult}
      future disc_res := actor<Group_manager>[v_c_g_id].get_fixed_discounts(ordered_item_ids);
       */
      val fixedDiscount: FutureRelation = groupId.transform( groupId => {
        val id = groupId.records.get.head.get(Customer.CustomerInfo.custGroupId).get
        val fixedDiscountRequest = Map(id -> GroupManager.GetFixedDiscounts.Request(orders.map(_.inventoryId)))
        Dactor.askDactor(system, classOf[GroupManager], fixedDiscountRequest)
      })
      // FutureRelation: i_id, fixed_disc

      /*
      SELECT session_id + 1 INTO v_session_id FROM cart_info;
      UPDATE cart_info
        SET c_id = o_c_id, session_id = session_id + 1;
       */
      // done outside of helper

      /*
      list<tuple> discounts := disc_res.get();
      results.value_list.when_all()

      foreach sec_id_res in results {
        foreach i_p in sec_id_res.second.get() {
          fixed_disc := lookup(discounts, i_p.i_id);
          i_quantity := lookup(orders, i_p.i_id);
          INSERT INTO cart_purchases
            //      sec_id,           session_id,   i_id, i_quantity, i_fixed_disc, i_min_price,   i_price
            VALUES (sec_id_res.first, v_session_id, i_id, i_quantity, fixed_disc,   i_p.min_price, i_ip.price);
        }
      }
       */
      val priceDisc: FutureRelation = priceList.innerJoin(fixedDiscount, (priceRec, discRec) =>
        priceRec.get(CartPurchases.inventoryId) == discRec.get(CartPurchases.inventoryId)
      )
      // FutureRelation: i_id, i_price, i_min_price, fixed_disc

      val orderRecordBuilder = Record(Set(CartPurchases.inventoryId, CartPurchases.sectionId, CartPurchases.quantity))
      val orderRelation: Relation = FutureRelation.fromRecordSeq(Future{orders.map(order => orderRecordBuilder(
        CartPurchases.inventoryId ~> order.inventoryId &
          CartPurchases.sectionId ~> order.sectionId &
          CartPurchases.quantity ~> order.quantity
      ).build())})
      val priceDiscOrder = priceDisc.innerJoin(orderRelation, (priceRec, orderRec) =>
        priceRec.get(CartPurchases.inventoryId) == orderRec.get(CartPurchases.inventoryId)
      )
      // FutureRelation: i_id, i_price, i_min_price, fixed_disc, sec_id, i_quantity

      priceDiscOrder.future
        .map(records => AddItemsHelper.Success(records.get, customerId, currentSessionId, replyTo))
        .pipeTo(recipient)
    }
  }
}

class Cart(id: Int) extends Dactor(id) {
  import Cart._

  private val timeout: Timeout = Timeout(2.seconds)

  override protected val relations: Map[RelationDef, MutableRelation] =
    Dactor.createAsRowRelations(Seq(CartInfo, CartPurchases))

  private var currentSessionId = 0

  override def receive: Receive = {
    case AddItems.Request(orders, customerId) =>
      AddItemsHelper(context.system, self, timeout).help(orders, customerId, currentSessionId, sender())

    case AddItemsHelper.Success(records, customerId, newSessionId, replyTo) =>
      updateAfterAddItems(records, customerId, newSessionId) match {
        case Success(_) => replyTo ! AddItems.Success(newSessionId)
        case Failure(e) => replyTo ! AddItems.Failure(e)
      }

    case Checkout.Request(_) => sender() ! Checkout.Failure(new NotImplementedError)
  }

  def updateAfterAddItems(records: Seq[Record], customerId: Int, newSessionId: Int): Try[Unit] = Try {
    relations(CartPurchases)
      .insertAll(records).get
    relations(CartInfo)
      .update(CartInfo.sessionId ~> newSessionId)
      .where[Int](CartInfo.customerId -> {
        _ == customerId
      }).get
  }
}
