package de.up.hpi.informationsystems.sampleapp.test.dactors

import java.time.{ZoneOffset, ZonedDateTime}

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.ColumnDef
import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._
import de.up.hpi.informationsystems.adbms.protocols.DefaultMessagingProtocol.InsertIntoRelation
import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.sampleapp.dactors.Cart.{CartInfo, CartPurchases}
import de.up.hpi.informationsystems.sampleapp.dactors.Customer.{CustomerInfo, StoreVisits}
import de.up.hpi.informationsystems.sampleapp.dactors.GroupManager.Discounts
import de.up.hpi.informationsystems.sampleapp.dactors.StoreSection.{Inventory, PurchaseHistory}
import de.up.hpi.informationsystems.sampleapp.dactors.{Cart, Customer, GroupManager, StoreSection}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.language.postfixOps

class SystemTest(_system: ActorSystem)
  extends TestKit(_system)
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  def this() = this(ActorSystem("CartTest"))

  override protected def afterAll(): Unit = shutdown(system)

  "A Cart" when {

    "filled with values" should {

      // Cart
      val cart42 = Dactor.dactorOf(system, classOf[Cart], 42)
      val cartInfo = Seq(
        CartInfo.newRecord(
          CartInfo.customerId ~> 22 &
          CartInfo.storeId ~> 1001 &
          CartInfo.sessionId ~> 32
        ).build()
      )
      val cartPurchases = Seq(
        CartPurchases.newRecord(
          CartPurchases.sectionId ~> 14 &
          CartPurchases.sessionId ~> 32 &
          CartPurchases.inventoryId ~> 2001 &
          CartPurchases.quantity ~> 1 &
          CartPurchases.fixedDiscount ~> 0 &
          CartPurchases.minPrice ~> 40.3 &
          CartPurchases.price ~> 60.99
        ).build(),
        CartPurchases.newRecord(
          CartPurchases.sectionId ~> 12 &
          CartPurchases.sessionId ~> 32 &
          CartPurchases.inventoryId ~> 32 &
          CartPurchases.quantity ~> 4 &
          CartPurchases.fixedDiscount ~> 10.5 &
          CartPurchases.minPrice ~> 22.5 &
          CartPurchases.price ~> 32.5
        ).build()
      )

      cart42 ! InsertIntoRelation(CartInfo.name, cartInfo)
      cart42 ! InsertIntoRelation(CartPurchases.name, cartPurchases)

      // Customer
      val customer22 = Dactor.dactorOf(system, classOf[Customer], 22)
      val customerInfo = Seq(
        CustomerInfo.newRecord(
          CustomerInfo.custName ~> "BOB" &
          CustomerInfo.custGroupId ~> 10
        ).build()
      )
      val storeVisits = Seq(
        StoreVisits.newRecord(
          StoreVisits.storeId ~> 12 &
          StoreVisits.timestamp ~> ZonedDateTime.of(2017, 5, 3, 12, 1, 32, 0, ZoneOffset.UTC) &
          StoreVisits.amount ~> 110 &
          StoreVisits.fixedDiscount ~> 15.5 &
          StoreVisits.varDiscount ~> 10.2
        ).build(),
        StoreVisits.newRecord(
          StoreVisits.storeId ~> 1001 &
          StoreVisits.timestamp ~> ZonedDateTime.of(2017, 5, 2, 11, 21, 27, 0, ZoneOffset.UTC) &
          StoreVisits.amount ~> 176.35 &
          StoreVisits.fixedDiscount ~> 22.45 &
          StoreVisits.varDiscount ~> 12.36
        ).build()
      )

      customer22 ! InsertIntoRelation(CustomerInfo.name, customerInfo)
      customer22 ! InsertIntoRelation(StoreVisits.name, storeVisits)

      // GroupManager
      val groupManager10 = Dactor.dactorOf(system, classOf[GroupManager], 10)
      val discounts = Seq(
        Discounts.newRecord(
          Discounts.id ~> 32 &
          Discounts.fixedDisc ~> 10.5
        ).build(),
        Discounts.newRecord(
          Discounts.id ~> 112 &
          Discounts.fixedDisc ~> 3.5
        ).build(),
        Discounts.newRecord(
          Discounts.id ~> 2001 &
          Discounts.fixedDisc ~> 3.5
        ).build()
      )

      groupManager10 ! InsertIntoRelation(Discounts.name, discounts)

      // StoreSection
      val storeSection14 = Dactor.dactorOf(system, classOf[StoreSection], 14)
      val inventory = Seq(
        Inventory.newRecord(
          Inventory.inventoryId ~> 2001 &
          Inventory.price ~> 60.99 &
          Inventory.minPrice ~> 40.3 &
          Inventory.quantity ~> 500 &
          Inventory.varDisc ~> 10.5
        ).build(),
        Inventory.newRecord(
          Inventory.inventoryId ~> 640 &
          Inventory.price ~> 27.99 &
          Inventory.minPrice ~> 20.5 &
          Inventory.quantity ~> 10 &
          Inventory.varDisc ~> 3.5
        ).build()
      )
      val purchaseHistory = Seq(
        PurchaseHistory.newRecord(
          PurchaseHistory.inventoryId ~> 2001 &
          PurchaseHistory.time ~> ZonedDateTime.of(2017, 5, 2, 11, 1, 32, 0, ZoneOffset.UTC) &
          PurchaseHistory.quantity ~> 2 &
          PurchaseHistory.customerId ~> 15
        ).build(),
        PurchaseHistory.newRecord(
          PurchaseHistory.inventoryId ~> 2001 &
          PurchaseHistory.time ~> ZonedDateTime.of(2017, 5, 2, 11, 21, 27, 0, ZoneOffset.UTC) &
          PurchaseHistory.quantity ~> 3 &
          PurchaseHistory.customerId ~> 22
        ).build(),
        PurchaseHistory.newRecord(
          PurchaseHistory.inventoryId ~> 640 &
          PurchaseHistory.time ~> ZonedDateTime.of(2017, 5, 2, 11, 21, 27, 0, ZoneOffset.UTC) &
          PurchaseHistory.quantity ~> 2 &
          PurchaseHistory.customerId ~> 22
        ).build()
      )

      storeSection14 ! InsertIntoRelation(Inventory.name, inventory)
      storeSection14 ! InsertIntoRelation(PurchaseHistory.name, purchaseHistory)

      "accept AddItem messages successfully" in {
        val probe = new TestProbe(system)

        cart42.tell(Cart.AddItems.Request(Seq(Cart.AddItems.Order(
          inventoryId = 2001,
          sectionId = 14,
          quantity = 20
        )), 22), probe.ref)
        probe.expectMsg(Cart.AddItems.Success(Seq(Record(Set(ColumnDef[Int]("session_id")))(ColumnDef[Int]("session_id") ~> 1).build())))

//        cart42.tell(Cart.AddItems.Request(Seq.empty, 22), probe.ref)
//        probe.expectMsg(Cart.AddItems.Success(Seq(Record(Set(ColumnDef[Int]("session_id")))(ColumnDef[Int]("session_id") ~> 2).build())))

      }
    }
  }
}
