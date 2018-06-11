package de.up.hpi.informationsystems.sampleapp.dactors

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import akka.actor.Props
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.adbms.protocols.RequestResponseProtocol
import de.up.hpi.informationsystems.sampleapp.dactors.Cart.CartPurchases

import scala.util.{Failure, Success, Try}

object StoreSection {
  // implicit default values
  import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._

  def props(id: Int): Props = Props(new StoreSection(id))

  object GetPrice {

    case class Request(inventoryIds: Seq[Int]) extends RequestResponseProtocol.Request
    // result: i_id, i_price, i_min_price
    case class Success(result: Seq[Record]) extends RequestResponseProtocol.Success
    case class Failure(e: Throwable) extends RequestResponseProtocol.Failure

  }

  object GetVariableDiscountUpdateInventory {
    val amountCol: ColumnDef[Long] = ColumnDef("amount")
    val fixedDiscCol: ColumnDef[Double] = ColumnDef("fixed_disc")
    val varDiscCol: ColumnDef[Double] = ColumnDef("var_disc")

    // order items: i_id, i_quantity, i_min_price, i_price, i_fixed_disc
    case class Request(customerId: Int, cartTime: ZonedDateTime, orderItems: Relation) extends RequestResponseProtocol.Request
    // result: amount, fixed_disc, var_disc
    case class Success(result: Seq[Record]) extends RequestResponseProtocol.Success
    case class Failure(e: Throwable) extends RequestResponseProtocol.Failure

  }

  object Inventory extends RelationDef {
    val inventoryId: ColumnDef[Int] = ColumnDef("i_id")
    val price: ColumnDef[Double] = ColumnDef("i_price")
    val minPrice: ColumnDef[Double] = ColumnDef("i_min_price")
    val quantity: ColumnDef[Long] = ColumnDef("i_quantity")
    val varDisc: ColumnDef[Double] = ColumnDef("i_var_disc")

    override val columns: Set[UntypedColumnDef] = Set(inventoryId, price, minPrice, quantity, varDisc)
    override val name: String = "inventory"
  }

  object PurchaseHistory extends RelationDef {
    val inventoryId: ColumnDef[Int] = ColumnDef("i_id")
    val time: ColumnDef[ZonedDateTime] = ColumnDef("time", ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC))
    val quantity: ColumnDef[Long] = ColumnDef("i_quantity")
    val customerId: ColumnDef[Int] = ColumnDef("c_id")

    override val columns: Set[UntypedColumnDef] = Set(inventoryId, time, quantity, customerId)
    override val name: String = "purchase_history"
  }
}

class StoreSection(id: Int) extends Dactor(id) {
  import StoreSection._

  override protected val relations: Map[RelationDef, MutableRelation] =
    Dactor.createAsRowRelations(Seq(Inventory, PurchaseHistory))

  override def receive: Receive = {
    case GetPrice.Request(inventoryIds) =>
      getPrice(inventoryIds) match {
        case Success(result) => sender() ! GetPrice.Success(result)
        case Failure(e) => sender() ! GetPrice.Failure(e)
      }

    case GetVariableDiscountUpdateInventory.Request(customerId, cartTime, orderItems) =>
      getVariableDiscountUpdateInventory(customerId, cartTime, orderItems) match {
        case Success(something) => sender() ! GetVariableDiscountUpdateInventory.Success(something)
        case Failure(e) => sender() ! GetVariableDiscountUpdateInventory.Failure(e)
      }
  }

  def getPrice(inventoryIds: Seq[Int]): Try[Seq[Record]] = {
    val resultSchema: Set[UntypedColumnDef] = Set(Inventory.inventoryId, Inventory.price, Inventory.minPrice)
    relations(Inventory)
      .project(resultSchema)
      .where[Int](Inventory.inventoryId -> { id => inventoryIds.contains(id) })
      .records
  }

  /**
    *
    * @param customerId the customer's id
    * @param cartTime   time at the beginning of checkout
    * @param orderItems i_id, i_quantity, i_min_price, i_price, i_fixed_disc
    * @return sequence of records containing orderItems with i_var_disc
    */
  def getVariableDiscountUpdateInventory(customerId: Int, cartTime: ZonedDateTime, orderItems: Relation): Try[Seq[Record]] = Try{
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

      import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
      relations(Inventory)
        .update(Inventory.quantity ~> (currentQuantity - item.get(CartPurchases.quantity).get))
        .where[Int]((Inventory.inventoryId, _ == item.get(CartPurchases.inventoryId).get))

      // add purchase to purchase history
      relations(PurchaseHistory)
        .insert(PurchaseHistory.newRecord(
          PurchaseHistory.inventoryId ~> item.get(CartPurchases.inventoryId).get &
          PurchaseHistory.time ~> cartTime &
          PurchaseHistory.quantity ~> item.get(CartPurchases.quantity).get &
          PurchaseHistory.customerId ~> customerId
        ).build())

      // calculate var_disc
      val recentSalesQuantities = relations(PurchaseHistory)
        .whereAll(Map(
          PurchaseHistory.inventoryId.untyped -> { _ == item.get(CartPurchases.inventoryId).get },
          PurchaseHistory.time.untyped -> { time: Any => time.asInstanceOf[ZonedDateTime].isAfter(cartTime.minusDays(7)) }  // k = 7
        ))
        .records.get
        .map(_.get(PurchaseHistory.quantity).getOrElse(0))

      val mean = recentSalesQuantities.size match {  // don't divide by zero
        case 0 => 0
        case size: Int  => recentSalesQuantities.foldLeft(0.0)(_+_.asInstanceOf[Long]) / size
      }
      val std_dev = math.sqrt(recentSalesQuantities.map(quantity => math.pow(quantity.asInstanceOf[Long].doubleValue() - mean, 2)).sum)

      // FIXME put the constants (K and C) at sensible places
      val C: Double = 0.1
      val varDiscFactor: Double =
        inventoryEntryForItem.get(Inventory.varDisc).get * (item.get(CartPurchases.quantity).get / (mean + C * std_dev))

      // FIXME check if amount - fixedDisc - varDisc < min_price and if yes possibly change amount

      // generate response format tuple
      val amount: Double = item.get(CartPurchases.price).get * item.get(CartPurchases.quantity).get
      val fixedDisc: Double = item.get(CartPurchases.fixedDiscount).get * amount
      val varDisc: Double = amount * varDiscFactor

      Record(Set(amountCol, fixedDiscCol, varDiscCol))(
        amountCol ~> (Math.round(amount * 100.0)/100.0) &
        fixedDiscCol ~> (Math.round(fixedDisc * 100.0)/100.0) &
        varDiscCol ~> (Math.round(varDisc * 100.0)/100.0)
      ).build()
    })
  }

}