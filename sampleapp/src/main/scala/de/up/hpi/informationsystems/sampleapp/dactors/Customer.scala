package de.up.hpi.informationsystems.sampleapp.dactors

import java.time.LocalDateTime

import akka.actor.Props
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.adbms.definition.Record.implicits._
import de.up.hpi.informationsystems.sampleapp.dactors.Customer.{AddStoreVisit, Authenticate, GetCustomerInfo}

import scala.util.{Failure, Success, Try}

object Customer {

  def props(name: String): Props = Props(new Customer(name))

  object GetCustomerInfo {

    case class Request()
    case class Success(results: Seq[Record])
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
}

class Customer(name: String) extends Dactor(name) {

  object CustomerInfo extends RowRelation {
    val custName: ColumnDef[String] = ColumnDef("cust_name")
    val custGroupId: ColumnDef[Int] = ColumnDef("c_g_id")

    override val columns: Set[UntypedColumnDef] = Set(custName, custGroupId)
  }

  object StoreVisits extends RowRelation {
    val storeId: ColumnDef[Int] = ColumnDef("store_id")
    val timestamp: ColumnDef[LocalDateTime] = ColumnDef("time")
    val amount: ColumnDef[Double] = ColumnDef("amount")
    val fixedDiscount: ColumnDef[Double] = ColumnDef("fixed_disc")
    val varDiscount: ColumnDef[Double] = ColumnDef("var_disc")

    override val columns: Set[UntypedColumnDef] = Set(storeId, timestamp, amount, fixedDiscount, varDiscount)
  }

  object Password extends RowRelation /* with encryption */ {
    val encryptedPassword: ColumnDef[String] = ColumnDef("enc_passwd")

    override val columns: Set[UntypedColumnDef] = Set(encryptedPassword)
  }

  override protected val relations: Map[String, Relation] =
    Map("customer_info" -> CustomerInfo) ++ Map("store_visits" -> StoreVisits) ++ Map("passwd" -> Password)

  override def receive: Receive = {
    case GetCustomerInfo.Request() =>
      getCustomerInfo() match {
        case Success(records) => sender() ! GetCustomerInfo.Success(records)
        case Failure(e) => sender() ! GetCustomerInfo.Failure(e)
      }

    case AddStoreVisit.Request(storeId: Int, time: LocalDateTime, amount: Double, fixedDiscount: Double, varDiscount: Double) =>
      addStoreVisit(storeId, time, amount, fixedDiscount, varDiscount) match {
        case Success(_) => sender() ! AddStoreVisit.Success()
        case Failure(e) => sender() ! AddStoreVisit.Failure(e)
      }

    case Authenticate.Request(passwordHash) =>
      authenticate(passwordHash) match {
        case true => sender() ! Authenticate.Success()
        case false => sender() ! Authenticate.Failure()
      }
  }

  def getCustomerInfo(): Try[Seq[Record]] = CustomerInfo.records

  def addStoreVisit(storeId: Int, time: LocalDateTime, amount: Double, fixedDiscount: Double, varDiscount: Double): Try[Record] =
    StoreVisits.insert(StoreVisits.newRecord(
      StoreVisits.storeId ~> storeId
      & StoreVisits.timestamp ~> time
      & StoreVisits.amount ~> amount
      & StoreVisits.fixedDiscount ~> fixedDiscount
      & StoreVisits.varDiscount ~> varDiscount
    ).build())

  def authenticate(passwordHash: String): Boolean = {
    val res = Password.where(Password.encryptedPassword -> { _.equals(passwordHash) }).records
    res.isFailure || res.get.length == 1
  }
}
