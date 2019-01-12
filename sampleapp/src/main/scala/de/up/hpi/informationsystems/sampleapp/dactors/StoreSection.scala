package de.up.hpi.informationsystems.sampleapp.dactors

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import akka.actor.Props
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.adbms.protocols.{DefaultMessageHandling, RequestResponseProtocol}
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, Relation}
import de.up.hpi.informationsystems.sampleapp.DataInitializer
import de.up.hpi.informationsystems.sampleapp.dactors.Cart.CartPurchases

import scala.util.{Failure, Success, Try}

object StoreSection {
  // implicit default values
  import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._

  def props(id: Int): Props = Props(new StoreSection(id))

  object GetPrice {
    sealed trait GetPrice extends RequestResponseProtocol.Message
    case class Request(inventoryIds: Seq[Int]) extends RequestResponseProtocol.Request[GetPrice]
    // result: i_id, i_price, i_min_price
    case class Success(result: Relation) extends RequestResponseProtocol.Success[GetPrice]
    case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[GetPrice]

  }

  object GetVariableDiscountUpdateInventory {
    val amountCol: ColumnDef[Long] = ColumnDef[Long]("amount")
    val fixedDiscCol: ColumnDef[Double] = ColumnDef[Double]("fixed_disc")
    val varDiscCol: ColumnDef[Double] = ColumnDef[Double]("var_disc")

    sealed trait GetVariableDiscountUpdateInventory extends RequestResponseProtocol.Message
    // order items: i_id, i_quantity, i_min_price, i_price, i_fixed_disc
    case class Request(customerId: Int, cartTime: ZonedDateTime, orderItems: Relation) extends RequestResponseProtocol.Request[GetVariableDiscountUpdateInventory]
    // result: amount, fixed_disc, var_disc
    case class Success(result: Relation) extends RequestResponseProtocol.Success[GetVariableDiscountUpdateInventory]
    case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[GetVariableDiscountUpdateInventory]

  }

  object GetAvailableQuantityFor {
    sealed trait GetAvailableQuantityFor extends RequestResponseProtocol.Message
    case class Request(inventoryId: Int) extends RequestResponseProtocol.Request[GetAvailableQuantityFor]

    case class Success(result: Relation) extends RequestResponseProtocol.Success[GetAvailableQuantityFor]
    case class Failure(e: Throwable) extends RequestResponseProtocol.Failure[GetAvailableQuantityFor]
  }

  object Inventory extends RelationDef {
    val inventoryId: ColumnDef[Int] = ColumnDef[Int]("i_id")
    val price: ColumnDef[Double] = ColumnDef[Double]("i_price")
    val minPrice: ColumnDef[Double] = ColumnDef[Double]("i_min_price")
    val quantity: ColumnDef[Long] = ColumnDef[Long]("i_quantity")
    val varDisc: ColumnDef[Double] = ColumnDef[Double]("i_var_disc")

    override val columns: Set[UntypedColumnDef] = Set(inventoryId, price, minPrice, quantity, varDisc)
    override val name: String = "inventory"
  }

  object PurchaseHistory extends RelationDef {
    val inventoryId: ColumnDef[Int] = ColumnDef[Int]("i_id")
    val time: ColumnDef[ZonedDateTime] = ColumnDef[ZonedDateTime]("time", ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC))
    val quantity: ColumnDef[Long] = ColumnDef[Long]("i_quantity")
    val customerId: ColumnDef[Int] = ColumnDef[Int]("c_id")

    override val columns: Set[UntypedColumnDef] = Set(inventoryId, time, quantity, customerId)
    override val name: String = "purchase_history"
  }

  class StoreSectionBase(id: Int) extends Dactor(id) {

    override protected val relations: Map[RelationDef, MutableRelation] =
      Dactor.createAsRowRelations(Seq(Inventory, PurchaseHistory))

    override def receive: Receive = {
      case GetPrice.Request(inventoryIds) =>
        getPrice(inventoryIds).records match {
          case Success(result) => sender() ! GetPrice.Success(Relation(result))
          case Failure(e) => sender() ! GetPrice.Failure(e)
        }

      case GetVariableDiscountUpdateInventory.Request(customerId, cartTime, orderItems) =>
        getVariableDiscountUpdateInventory(customerId, cartTime, orderItems) match {
          case Success(totals: Seq[Record]) => sender() ! GetVariableDiscountUpdateInventory.Success(Relation(totals))
          case Failure(e) => sender() ! GetVariableDiscountUpdateInventory.Failure(e)
        }

      case GetAvailableQuantityFor.Request(inventoryId) =>
        getAvailableQuantityFor(inventoryId).records match {
          case Success(result) => sender() ! GetAvailableQuantityFor.Success(Relation(result))
          case Failure(e) => sender() ! GetAvailableQuantityFor.Failure(e)
        }
    }

    def getAvailableQuantityFor(inventoryId: Int): Relation = {
      relations(Inventory)
        .where[Int](Inventory.inventoryId -> { _ == inventoryId })
        .project(Set[UntypedColumnDef](Inventory.quantity))
    }

    def getPrice(inventoryIds: Seq[Int]): Relation = {
      val resultSchema: Set[UntypedColumnDef] = Set(Inventory.inventoryId, Inventory.price, Inventory.minPrice)
      relations(Inventory)
        .project(resultSchema)
        .where[Int](Inventory.inventoryId -> { id => inventoryIds.contains(id) })
    }

    /**
      *
      * @param customerId the customer's id
      * @param cartTime   time at the beginning of checkout
      * @param orderItems i_id, i_quantity, i_min_price, i_price, i_fixed_disc
      * @return sequence of records containing orderItems with i_var_disc
      */
    def getVariableDiscountUpdateInventory(customerId: Int, cartTime: ZonedDateTime, orderItems: Relation): Try[Seq[Record]] = Try {
      val C: Double = 0.1
      val K: Int = 7

      // response columns
      val amountCol = ColumnDef[Double]("amount", 0.0)
      val fixedDiscCol = ColumnDef[Double]("fixed_disc", 0.0)
      val varDiscCol = ColumnDef[Double]("var_disc", 0.0)

      // for each of the orderitems calculate the variable discounts generate response format tuple
      orderItems.records.get.map(item => {
        // update inventory for this item
        val inventoryEntryForItem = relations(Inventory)
          .where[Int]((Inventory.inventoryId, _ == item.get(CartPurchases.inventoryId).get))
          .records.get.head

        val currentQuantity = inventoryEntryForItem.get(Inventory.quantity).get

        import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
        relations(Inventory)
          .update(Inventory.quantity ~> (currentQuantity - item.get(CartPurchases.quantity).get))
          .where[Int]((Inventory.inventoryId, _ == item.get(CartPurchases.inventoryId).get))

        // get recent sales to calculate var_disc
        val recentSalesQuantities = relations(PurchaseHistory)
          .whereAll(Map(
            PurchaseHistory.inventoryId -> {
              _ == item.get(CartPurchases.inventoryId).get
            },
            PurchaseHistory.time -> { time => time.asInstanceOf[ZonedDateTime].isAfter(cartTime.minusDays(K)) }
          ))
          .records.get
          .map(_.get(PurchaseHistory.quantity).getOrElse(0))

        // add purchase to purchase history
        relations(PurchaseHistory)
          .insert(PurchaseHistory.newRecord(
            PurchaseHistory.inventoryId ~> item.get(CartPurchases.inventoryId).get &
              PurchaseHistory.time ~> cartTime &
              PurchaseHistory.quantity ~> item.get(CartPurchases.quantity).get &
              PurchaseHistory.customerId ~> customerId
          ).build())

        // calculate var_disc
        val mean = recentSalesQuantities.size match { // don't divide by zero
          case 0 => 0
          case size: Int => recentSalesQuantities.foldLeft(0.0)(_ + _.asInstanceOf[Long]) / size
        }
        val std_dev = math.sqrt(recentSalesQuantities.map(quantity => math.pow(quantity.asInstanceOf[Long].doubleValue() - mean, 2)).sum)

        val varDiscFactor: Double = mean + C * std_dev match {
          case 0 => 0
          case divisor: Double => inventoryEntryForItem.get(Inventory.varDisc).get * item.get(CartPurchases.quantity).get / divisor
        }

        // IGNORE check if amount - fixedDisc - varDisc < min_price and if yes possibly change amount

        // generate response format tuple
        val amount: Double = item.get(CartPurchases.price).get * item.get(CartPurchases.quantity).get
        val fixedDisc: Double = item.get(CartPurchases.fixedDiscount).get * amount
        val varDisc: Double = amount * varDiscFactor

        Record(Set(amountCol, fixedDiscCol, varDiscCol))(
          amountCol ~> (Math.round(amount * 100.0) / 100.0) &
            fixedDiscCol ~> (Math.round(fixedDisc * 100.0) / 100.0) &
            varDiscCol ~> (Math.round(varDisc * 100.0) / 100.0)
        ).build()
      })
    }
  }

}

class StoreSection(id: Int)
  extends StoreSection.StoreSectionBase(id)
    with DataInitializer
    with DefaultMessageHandling
