package de.up.hpi.informationsystems.sampleapp.dactors

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import akka.actor.Props
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.adbms.protocols.{DefaultMessageHandling, RequestResponseProtocol}
import de.up.hpi.informationsystems.sampleapp.DataInitializer

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

class StoreSectionBase(id: Int) extends Dactor(id) {
  import StoreSection._

  override protected val relations: Map[RelationDef, MutableRelation] =
    Dactor.createAsRowRelations(Seq(Inventory, PurchaseHistory))

  override def receive: Receive = {
    case GetPrice.Request(inventoryIds) =>
      getPrice(inventoryIds) match {
        case Success(result) => sender() ! GetPrice.Success(result)
        case Failure(e) => sender() ! GetPrice.Failure(e)
      }

    case GetVariableDiscountUpdateInventory.Request =>
      sender() ! GetVariableDiscountUpdateInventory.Failure(new NotImplementedError)

  }

  def getPrice(inventoryIds: Seq[Int]): Try[Seq[Record]] = {
    val resultSchema: Set[UntypedColumnDef] = Set(Inventory.inventoryId, Inventory.price, Inventory.minPrice)
    relations(Inventory)
      .project(resultSchema)
      .where[Int](Inventory.inventoryId -> { id => inventoryIds.contains(id) })
      .records
  }

}

class StoreSection(id: Int)
  extends StoreSectionBase(id)
    with DataInitializer // DataInitializer needs to be mixed-in here: Trait Linearization!
    with DefaultMessageHandling