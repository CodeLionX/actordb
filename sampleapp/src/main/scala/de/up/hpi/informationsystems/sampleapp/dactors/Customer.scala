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

  object CustomerInfoDef extends RelationDef {
    val custName: ColumnDef[String] = ColumnDef("cust_name")
    val custGroupId: ColumnDef[Int] = ColumnDef("c_g_id")

    override val columns: Set[UntypedColumnDef] = Set(custName, custGroupId)
    override val name: String = "customer_info"
  }

  object StoreVisitsDef extends RelationDef {
    val storeId: ColumnDef[Int] = ColumnDef("store_id")
    val timestamp: ColumnDef[LocalDateTime] = ColumnDef("time")
    val amount: ColumnDef[Double] = ColumnDef("amount")
    val fixedDiscount: ColumnDef[Double] = ColumnDef("fixed_disc")
    val varDiscount: ColumnDef[Double] = ColumnDef("var_disc")

    override val columns: Set[UntypedColumnDef] = Set(storeId, timestamp, amount, fixedDiscount, varDiscount)
    override val name: String = "store_visits"
  }

  object PasswordDef extends RelationDef /* with Encryption */ {
    val encryptedPassword: ColumnDef[String] = ColumnDef("enc_passwd")

    override val columns: Set[UntypedColumnDef] = Set(encryptedPassword)
    override val name: String = "passwd"
  }

}

class Customer(id: Int) extends Dactor(id) {
  import Customer._

  val CustomerInfo = RowRelation(CustomerInfoDef)
  val StoreVisits = RowRelation(StoreVisitsDef)
  val Password = RowRelation(PasswordDef)

  override protected val relations: Map[String, MutableRelation] = Map(CustomerInfoDef.name -> CustomerInfo) ++
    Map(StoreVisitsDef.name -> StoreVisits) ++
    Map(PasswordDef.name -> Password)

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
    StoreVisits.insert(StoreVisitsDef.newRecord(
      StoreVisitsDef.storeId ~> storeId
        & StoreVisitsDef.timestamp ~> time
        & StoreVisitsDef.amount ~> amount
        & StoreVisitsDef.fixedDiscount ~> fixedDiscount
        & StoreVisitsDef.varDiscount ~> varDiscount
    ).build())

  def authenticate(passwordHash: String): Boolean = {
    val res = Password.where[String](PasswordDef.encryptedPassword -> {
      _.equals(passwordHash)
    }).records
    res.isFailure || res.get.length == 1
  }
}
