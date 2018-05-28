package de.up.hpi.informationsystems.sampleapp.dactors

import akka.actor.Props
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition._

import scala.util.{Failure, Success, Try}

object GroupManager {

  def props(id: Int): Props = Props(new GroupManager(id))

  object GetFixedDiscounts {

    case class Request(ids: Seq[Int])
    case class Success(results: Seq[Record])
    // results: i_id, fixed_disc
    case class Failure(e: Throwable)

  }
}

class GroupManager(id: Int) extends Dactor(id) {
  import GroupManager._

  object Discounts extends RowRelation {
    val id: ColumnDef[Int] = ColumnDef("i_id")
    val fixedDisc: ColumnDef[Double] = ColumnDef("fixed_disc")

    override val columns: Set[UntypedColumnDef] = Set(id, fixedDisc)
  }

  override protected val relations: Map[String, MutableRelation] = Map("discounts" -> Discounts)

  override def receive: Receive = {
    case GetFixedDiscounts.Request(ids) =>
      getFixedDiscounts(ids) match {
        case Success(result) => sender() ! GetFixedDiscounts.Success(result)
        case Failure(e) => sender() ! GetFixedDiscounts.Failure(e)
      }
  }

  def getFixedDiscounts(ids: Seq[Int]): Try[Seq[Record]] =
    Discounts
      .where(Discounts.id -> { id: Int => ids.contains(id) })
      .records

}
