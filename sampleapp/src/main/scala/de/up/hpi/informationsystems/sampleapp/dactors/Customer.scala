package de.up.hpi.informationsystems.sampleapp.dactors

import java.time.LocalDateTime

import akka.actor.Props
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.adbms.definition.ColumnCellMapping._

import scala.util.{Failure, Success, Try}

object Customer {

  def props(id: Int): Props = Props(new Customer(id))

  object GetCustomerInfo {

    case class Request()
    case class Success(result: Record)
    case class Failure(e: Throwable)

  }

  object GetCustomerGroupId {

    case class Request()
    case class Success(result: Int)
    case class Failure(e: Throwable)

  }

  object AddStoreVisit {

    case class Request(storeId: Int, time: LocalDateTime, amount: Double, fixedDiscount: Double, varDiscount: Double)
    case class Success()
    case class Failure(e: Throwable)

  }

  object Authenticate {

    case class Request(passwordHash: String)
    case class Success()
    case class Failure()

  }

  object CustomerInfo extends RelationDef {
    val custName: ColumnDef[String] = ColumnDef("cust_name")
    val custGroupId: ColumnDef[Int] = ColumnDef("c_g_id")

    override val columns: Set[UntypedColumnDef] = Set(custName, custGroupId)
    override val name: String = "customer_info"
  }

  object StoreVisits extends RelationDef {
    val storeId: ColumnDef[Int] = ColumnDef("store_id")
    val timestamp: ColumnDef[LocalDateTime] = ColumnDef("time")
    val amount: ColumnDef[Double] = ColumnDef("amount")
    val fixedDiscount: ColumnDef[Double] = ColumnDef("fixed_disc")
    val varDiscount: ColumnDef[Double] = ColumnDef("var_disc")

    override val columns: Set[UntypedColumnDef] = Set(storeId, timestamp, amount, fixedDiscount, varDiscount)
    override val name: String = "store_visits"
  }

  object Password extends RelationDef /* with Encryption */ {
    val encryptedPassword: ColumnDef[String] = ColumnDef("enc_passwd")

    override val columns: Set[UntypedColumnDef] = Set(encryptedPassword)
    override val name: String = "passwd"
  }

}

class Customer(id: Int) extends Dactor(id) {
  import Customer._

  val customerInfo = RowRelation(CustomerInfo)
  val storeVisits = RowRelation(StoreVisits)
  val password = RowRelation(Password)

  override protected val relations: Map[String, MutableRelation] = Map(CustomerInfo.name -> customerInfo) ++
    Map(StoreVisits.name -> storeVisits) ++
    Map(Password.name -> password)

  override def receive: Receive = {
    case GetCustomerInfo.Request() =>
      getCustomerInfo match {
        case Success(record) => sender() ! GetCustomerInfo.Success(record)
        case Failure(e) => sender() ! GetCustomerInfo.Failure(e)
      }

    case GetCustomerGroupId.Request() =>
      getCustomerGroupId match {
        case Success(groupId) => sender() ! GetCustomerGroupId.Success(groupId)
        case Failure(e) => sender() ! GetCustomerGroupId.Failure(e)
      }

    case AddStoreVisit.Request(storeId: Int, time: LocalDateTime, amount: Double, fixedDiscount: Double, varDiscount: Double) =>
      addStoreVisit(storeId, time, amount, fixedDiscount, varDiscount) match {
        case Success(_) => sender() ! AddStoreVisit.Success()
        case Failure(e) => sender() ! AddStoreVisit.Failure(e)
      }

    case Authenticate.Request(passwordHash) =>
      if (authenticate(passwordHash)) {
        sender() ! Authenticate.Success()
      } else {
        sender() ! Authenticate.Failure()
      }
  }

  def getCustomerInfo: Try[Record] = {
    val rowCount = customerInfo.records.get.size
    if (rowCount > 1) {
      throw InconsistentStateException(s"this relation was expected to contain at maximum 1 row, but contained $rowCount")
    }
    Try(customerInfo.records.get.head)
  }

  def getCustomerGroupId: Try[Int] = {
    val rowCount = customerInfo.records.get.size
    if (rowCount > 1) {
      throw InconsistentStateException(s"this relation was expected to contain at maximum 1 row, but contained $rowCount")
    }
    Try(customerInfo.records.get.head.get(CustomerInfo.custGroupId).get)
  }

  def addStoreVisit(storeId: Int, time: LocalDateTime, amount: Double, fixedDiscount: Double, varDiscount: Double): Try[Record] =
    storeVisits.insert(StoreVisits.newRecord(
      StoreVisits.storeId ~> storeId
        & StoreVisits.timestamp ~> time
        & StoreVisits.amount ~> amount
        & StoreVisits.fixedDiscount ~> fixedDiscount
        & StoreVisits.varDiscount ~> varDiscount
    ).build())

  def authenticate(passwordHash: String): Boolean = {
    val res = password.where[String](Password.encryptedPassword -> {
      _.equals(passwordHash)
    }).records
    res.isFailure || res.get.length == 1
  }
}
