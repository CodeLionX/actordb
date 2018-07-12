package de.up.hpi.informationsystems.adbms.relation

import de.up.hpi.informationsystems.adbms.definition.ColumnDef

object RelationBinOps {

  def innerJoin(left: Relation, right: Relation, on: Relation.RecordComparator): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.innerJoin(tr1, tr2, on)
//    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.innerJoin(fr, tr, on)
//    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.innerJoin(mr, tr, on)
//    case (fr: FutureRelation, r: Relation) => FutureRelation.BinOps.innerJoin(fr, r, on)
//    case (mr: MutableRelation, r: Relation) => MutableRelation.BinOps.innerJoin(mr, r, on)
  }

  def leftJoin(left: Relation, right: Relation, on: Relation.RecordComparator): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.leftJoin(tr1, tr2, on)
  }

  def rightJoin(left: Relation, right: Relation, on: Relation.RecordComparator): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.rightJoin(tr1, tr2, on)
  }

  def outerJoin(left: Relation, right: Relation, on: Relation.RecordComparator): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.outerJoin(tr1, tr2, on)
  }

  def innerEquiJoin[T](left: Relation, right: Relation, on: (ColumnDef[T], ColumnDef[T])): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.innerEquiJoin(tr1, tr2, on)
  }

  def unionAll(left: Relation, right: Relation): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.unionAll(tr1, tr2)
  }

  def union(left: Relation, right: Relation): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.union(tr1, tr2)
  }
}
