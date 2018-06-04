package de.up.hpi.informationsystems.sampleapp.dactors

import akka.actor.Props
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition._

import scala.util.{Failure, Success, Try}

object GroupManager {

  def props(id: Int): Props = Props(new GroupManager(id))

  object GetFixedDiscounts {

    case class Request(ids: Seq[Int])
    // results: i_id, fixed_disc
    case class Success(results: Seq[Record])
    case class Failure(e: Throwable)

  }

  object Discounts extends RelationDef {
    val id: ColumnDef[Int] = ColumnDef("i_id")
    val fixedDisc: ColumnDef[Double] = ColumnDef("fixed_disc")

    override val columns: Set[UntypedColumnDef] = Set(id, fixedDisc)
    override val name: String = "discounts"
  }
}

class GroupManager(id: Int) extends Dactor(id) {
  import GroupManager._

  override protected val relations: Map[RelationDef, MutableRelation] =
    Dactor.createAsRowRelations(Seq(Discounts))

  override def receive: Receive = {
    case GetFixedDiscounts.Request(ids) =>
      getFixedDiscounts(ids) match {
        case Success(result) => sender() ! GetFixedDiscounts.Success(result)
        case Failure(e) => sender() ! GetFixedDiscounts.Failure(e)
      }
  }

  def getFixedDiscounts(ids: Seq[Int]): Try[Seq[Record]] =
    relations(Discounts)
      .where(Discounts.id -> { id: Int => ids.contains(id) })
      .records

}
