package de.up.hpi.informationsystems.fouleggs.dactors

import akka.actor.{Actor => AkkaActor}
import de.up.hpi.informationsystems.adbms.Dactor
import de.up.hpi.informationsystems.adbms.definition.ColumnDef.UntypedColumnDef
import de.up.hpi.informationsystems.adbms.definition.{ColumnDef, RelationDef}
import de.up.hpi.informationsystems.adbms.definition.ColumnTypeDefaults._
import de.up.hpi.informationsystems.adbms.protocols.DefaultMessageHandling
import de.up.hpi.informationsystems.adbms.relation.{MutableRelation, RowRelation, SingleRowRelation}


object Scoring {

  object Info extends RelationDef {
    val movieId: ColumnDef[Int] = ColumnDef[Int]("movie_id")
    val avgScore: ColumnDef[Double] = ColumnDef[Double]("avg_score")
    val count: ColumnDef[Long] = ColumnDef[Long]("count")

    override val name: String = "scoring_info"
    override val columns: Set[UntypedColumnDef] = Set(movieId, avgScore, count)
  }

  object Reviews extends RelationDef {
    val author: ColumnDef[String] = ColumnDef[String]("author")
    val text: ColumnDef[String] = ColumnDef[String]("text")
    val score: ColumnDef[Double] = ColumnDef[Double]("score")

    override val name: String = "reviews"
    override val columns: Set[UntypedColumnDef] = Set(author, text, score)
  }

  class ScoringBase(id: Int) extends Dactor(id) {

    override protected val relations: Map[RelationDef, MutableRelation] =
      Map(Info -> SingleRowRelation(Info)) ++
        Map(Reviews -> RowRelation(Reviews))

    override def receive: Receive = AkkaActor.emptyBehavior

  }
}

class Scoring(id: Int)
  extends Scoring.ScoringBase(id)
    with DefaultMessageHandling
