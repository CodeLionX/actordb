package de.up.hpi.informationsystems.sampleapp.test.dactors

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.definition.Record
import de.up.hpi.informationsystems.adbms.protocols.DefaultMessagingProtocol.InsertIntoRelation
import de.up.hpi.informationsystems.sampleapp.dactors.StoreSection
import de.up.hpi.informationsystems.sampleapp.dactors.StoreSection.Inventory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class StoreSectionTest(_system: ActorSystem)
  extends TestKit(_system)
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  def this() = this(ActorSystem("StoreSectionTest"))

  override protected def afterAll(): Unit = shutdown(system)

  "A StoreSection" when {

    "filled with items in the inventory and no purchase history" should {
      // StoreSection
      val storeSection1 = Dactor.dactorOf(system, classOf[StoreSection], 1)
      val inventory1 = Seq(
        Inventory.newRecord(
          Inventory.inventoryId ~> 100 &
          Inventory.price ~> 9.99 &
          Inventory.minPrice ~> 6.39 &
          Inventory.quantity ~> 1000 &
          Inventory.varDisc ~> 0.1
        ).build(),
        Inventory.newRecord(
          Inventory.inventoryId ~> 101 &
            Inventory.price ~> 19.99 &
            Inventory.minPrice ~> 14.00 &
            Inventory.quantity ~> 500 &
            Inventory.varDisc ~> 0.2
        ).build()
      )
      storeSection1 ! InsertIntoRelation(Inventory.name, inventory1)

      "accept GetPrice messages successfully" in {
        val probe = new TestProbe(system)
        storeSection1.tell(StoreSection.GetPrice.Request(Seq(100, 101)), probe.ref)
        probe.expectMsg(StoreSection.GetPrice.Success(Seq(
          Record(Set(Inventory.inventoryId, Inventory.price, Inventory.minPrice))(
            Inventory.inventoryId ~> 100 &
            Inventory.price ~> 9.99 &
            Inventory.minPrice ~> 6.39
          ).build(),
          Record(Set(Inventory.inventoryId, Inventory.price, Inventory.minPrice))(
            Inventory.inventoryId ~> 101 &
            Inventory.price ~> 19.99 &
            Inventory.minPrice ~> 14.00
          ).build()
        )))
      }

      "return empty when calling GetPrice for non-existent inventoryIds" in {
        val probe = new TestProbe(system)
        storeSection1.tell(StoreSection.GetPrice.Request(Seq(10, 11)), probe.ref)
        probe.expectMsg(StoreSection.GetPrice.Success(Seq.empty))
      }

      // TODO Failure cases
    }
  }

}
