package de.up.hpi.informationsystems.adbms.relation

import de.up.hpi.informationsystems.adbms.definition.ColumnDef

object RelationBinOps {

  def innerJoin(left: Relation, right: Relation, on: Relation.RecordComparator): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.innerJoin(tr1, tr2, on)
    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.innerJoin(fr, tr, on)
    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.innerJoin(mr, tr, on)
    case (_: TransientRelation, r) => throw new UnsupportedOperationException(s"Binary relation operation for ${r.getClass.getSimpleName} is not yet supported!")
    case (fr: FutureRelation, r) => FutureRelation.BinOps.innerJoin(fr, r, on)
    case (mr: MutableRelation, r) => MutableRelation.BinOps.innerJoin(mr, r, on)
  }

  def leftJoin(left: Relation, right: Relation, on: Relation.RecordComparator): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.leftJoin(tr1, tr2, on)
    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.leftJoin(fr, tr, on)
    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.leftJoin(mr, tr, on)
    case (fr: FutureRelation, r: Relation) => FutureRelation.BinOps.leftJoin(fr, r, on)
    case (mr: MutableRelation, r: Relation) => MutableRelation.BinOps.leftJoin(mr, r, on)
  }

  def rightJoin(left: Relation, right: Relation, on: Relation.RecordComparator): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.rightJoin(tr1, tr2, on)
    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.rightJoin(fr, tr, on)
    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.rightJoin(mr, tr, on)
    case (fr: FutureRelation, r: Relation) => FutureRelation.BinOps.rightJoin(fr, r, on)
    case (mr: MutableRelation, r: Relation) => MutableRelation.BinOps.rightJoin(mr, r, on)
  }

  def outerJoin(left: Relation, right: Relation, on: Relation.RecordComparator): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.outerJoin(tr1, tr2, on)
    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.outerJoin(fr, tr, on)
    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.outerJoin(mr, tr, on)
    case (fr: FutureRelation, r: Relation) => FutureRelation.BinOps.outerJoin(fr, r, on)
    case (mr: MutableRelation, r: Relation) => MutableRelation.BinOps.outerJoin(mr, r, on)
  }

  def innerEquiJoin[T](left: Relation, right: Relation, on: (ColumnDef[T], ColumnDef[T])): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.innerEquiJoin(tr1, tr2, on)
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.innerEquiJoin(tr1, tr2, on)
    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.innerEquiJoin(fr, tr, on)
    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.innerEquiJoin(mr, tr, on)
    case (fr: FutureRelation, r: Relation) => FutureRelation.BinOps.innerEquiJoin(fr, r, on)
    case (mr: MutableRelation, r: Relation) => MutableRelation.BinOps.innerEquiJoin(mr, r, on)
  }

  def unionAll(left: Relation, right: Relation): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.unionAll(tr1, tr2)
    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.unionAll(fr, tr)
    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.unionAll(mr, tr)
    case (fr: FutureRelation, r: Relation) => FutureRelation.BinOps.unionAll(fr, r)
    case (mr: MutableRelation, r: Relation) => MutableRelation.BinOps.unionAll(mr, r)
  }

  def union(left: Relation, right: Relation): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.union(tr1, tr2)
    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.union(fr, tr)
    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.union(mr, tr)
    case (fr: FutureRelation, r: Relation) => FutureRelation.BinOps.union(fr, r)
    case (mr: MutableRelation, r: Relation) => MutableRelation.BinOps.union(mr, r)
  }
}
