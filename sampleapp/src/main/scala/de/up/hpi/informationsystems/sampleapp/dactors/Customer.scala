package de.up.hpi.informationsystems.sampleapp.dactors

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import akka.actor.Props
import de.up.hpi.informationsystems.adbms.{Dactor, InconsistentStateException}
import de.up.hpi.informationsystems.adbms.record.ColumnCellMapping._
import de.up.hpi.informationsystems.adbms.definition._
import de.up.hpi.informationsystems.adbms.protocols.{DefaultMessageHandling, RequestResponseProtocol}
import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, Relation}
import de.up.hpi.informationsystems.sampleapp.{AuthenticationFailedException, DataInitializer}

import scala.util.{Failure, Success, Try}

object Customer {
  // implicit default values
  import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._

  def props(id: Int): Props = Props(new Customer(id))

  object GetCustomerInfo {

    case class Request() extends RequestResponseProtocol.Request
    case class Success(result: Relation) extends RequestResponseProtocol.Success
    case class Failure(e: Throwable) extends RequestResponseProtocol.Failure

  }

  object GetCustomerGroupId {

    case class Request() extends RequestResponseProtocol.Request
    case class Success(result: Relation) extends RequestResponseProtocol.Success
    case class Failure(e: Throwable) extends RequestResponseProtocol.Failure

  }

  object AddStoreVisit {

    case class Request(storeId: Int, time: ZonedDateTime, amount: Double, fixedDiscount: Double, varDiscount: Double) extends RequestResponseProtocol.Request
    case class Success(result: Relation) extends RequestResponseProtocol.Success
    case class Failure(e: Throwable) extends RequestResponseProtocol.Failure

  }

  object Authenticate {

    case class Request(passwordHash: String) extends RequestResponseProtocol.Request
    case class Success(result: Relation) extends RequestResponseProtocol.Success
    case class Failure(e: Throwable) extends RequestResponseProtocol.Failure

  }

  object CustomerInfo extends RelationDef {
    val custName: ColumnDef[String] = ColumnDef("cust_name")
    val custGroupId: ColumnDef[Int] = ColumnDef("c_g_id")

    override val columns: Set[UntypedColumnDef] = Set(custName, custGroupId)
    override val name: String = "customer_info"
  }

  object StoreVisits extends RelationDef {
    val storeId: ColumnDef[Int] = ColumnDef("store_id")
    val timestamp: ColumnDef[ZonedDateTime] = ColumnDef("time", ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC))
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

  class CustomerBase(id: Int) extends Dactor(id) {

    override protected val relations: Map[RelationDef, MutableRelation] =
      Dactor.createAsRowRelations(Seq(CustomerInfo, StoreVisits, Password))

    override def receive: Receive = {
      case GetCustomerInfo.Request() =>
        getCustomerInfo match {
          case Success(customerInfo) => sender() ! GetCustomerInfo.Success(customerInfo)
          case Failure(e) => sender() ! GetCustomerInfo.Failure(e)
        }

      case GetCustomerGroupId.Request() =>
        getCustomerGroupId match {
          case Success(groupId) => sender() ! GetCustomerGroupId.Success(groupId)
          case Failure(e) => sender() ! GetCustomerGroupId.Failure(e)
        }

      case AddStoreVisit.Request(storeId: Int, time: ZonedDateTime, amount: Double, fixedDiscount: Double, varDiscount: Double) =>
        addStoreVisit(storeId, time, amount, fixedDiscount, varDiscount) match {
          case Success(_) => sender() ! AddStoreVisit.Success(Relation(Seq.empty))
          case Failure(e) => sender() ! AddStoreVisit.Failure(e)
        }

      case Authenticate.Request(passwordHash) =>
        if (authenticate(passwordHash)) {
          sender() ! Authenticate.Success(Relation(Seq.empty))
        } else {
          sender() ! Authenticate.Failure(AuthenticationFailedException("failed to authenticate using password"))
        }
    }

    def getCustomerInfo: Try[Relation] = {
      val rowCount = relations(CustomerInfo).records.get.size
      if (rowCount > 1) {
        throw InconsistentStateException(s"this relation was expected to contain at maximum 1 row, but contained $rowCount")
      }
      Try(Relation(relations(CustomerInfo).records.get))
    }

    def getCustomerGroupId: Try[Relation] = {
      val rowCount = relations(CustomerInfo).records.get.size
      if (rowCount > 1) {
        throw InconsistentStateException(s"this relation was expected to contain at maximum 1 row, but contained $rowCount")
      }
      Try(relations(CustomerInfo)
        .project(Set(CustomerInfo.custGroupId)))
    }

    def addStoreVisit(storeId: Int, time: ZonedDateTime, amount: Double, fixedDiscount: Double, varDiscount: Double): Try[Record] =
      relations(StoreVisits).insert(StoreVisits.newRecord(
        StoreVisits.storeId ~> storeId
          & StoreVisits.timestamp ~> time
          & StoreVisits.amount ~> amount
          & StoreVisits.fixedDiscount ~> fixedDiscount
          & StoreVisits.varDiscount ~> varDiscount
      ).build())

    def authenticate(passwordHash: String): Boolean = {
      val res = relations(Password).where[String](Password.encryptedPassword -> {
        _.equals(passwordHash)
      }).records
      res.isFailure || res.get.length == 1
    }
  }
}

class Customer(id: Int)
  extends Customer.CustomerBase(id)
    with DataInitializer
    with DefaultMessageHandling