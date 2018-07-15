package de.up.hpi.informationsystems.adbms.relation

import de.up.hpi.informationsystems.adbms.definition.ColumnDef

object RelationBinOps {

  def innerJoin(left: Relation, right: Relation, on: Relation.RecordComparator): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.innerJoin(tr1, tr2, on)
    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.innerJoin(fr, tr, on)
    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.innerJoin(mr, tr, on)
    case (fr: FutureRelation, r) => FutureRelation.BinOps.innerJoin(fr, r, on)
    case (mr: MutableRelation, fr: FutureRelation) => FutureRelation.BinOps.innerJoin(fr, mr, on)
    case (mr: MutableRelation, r) => MutableRelation.BinOps.innerJoin(mr, r, on)
    case (r1, r2) => throw new UnsupportedOperationException(s"Binary relation operation for ${r1.getClass.getSimpleName} and ${r2.getClass.getSimpleName} is not yet supported!")
  }

  def leftJoin(left: Relation, right: Relation, on: Relation.RecordComparator): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.leftJoin(tr1, tr2, on)
    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.rightJoin(fr, tr, on)
    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.rightJoin(mr, tr, on)
    case (fr: FutureRelation, r) => FutureRelation.BinOps.leftJoin(fr, r, on)
    case (mr: MutableRelation, fr: FutureRelation) => FutureRelation.BinOps.rightJoin(fr, mr, on)
    case (mr: MutableRelation, r) => MutableRelation.BinOps.leftJoin(mr, r, on)
    case (r1, r2) => throw new UnsupportedOperationException(s"Binary relation operation for ${r1.getClass.getSimpleName} and ${r2.getClass.getSimpleName} is not yet supported!")
  }

  def rightJoin(left: Relation, right: Relation, on: Relation.RecordComparator): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.rightJoin(tr1, tr2, on)
    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.leftJoin(fr, tr, on)
    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.leftJoin(mr, tr, on)
    case (fr: FutureRelation, r) => FutureRelation.BinOps.rightJoin(fr, r, on)
    case (mr: MutableRelation, fr: FutureRelation) => FutureRelation.BinOps.leftJoin(fr, mr, on)
    case (mr: MutableRelation, r) => MutableRelation.BinOps.rightJoin(mr, r, on)
    case (r1, r2) => throw new UnsupportedOperationException(s"Binary relation operation for ${r1.getClass.getSimpleName} and ${r2.getClass.getSimpleName} is not yet supported!")
  }

  def outerJoin(left: Relation, right: Relation, on: Relation.RecordComparator): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.outerJoin(tr1, tr2, on)
    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.outerJoin(fr, tr, on)
    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.outerJoin(mr, tr, on)
    case (fr: FutureRelation, r) => FutureRelation.BinOps.outerJoin(fr, r, on)
    case (mr: MutableRelation, fr: FutureRelation) => FutureRelation.BinOps.outerJoin(fr, mr, on)
    case (mr: MutableRelation, r) => MutableRelation.BinOps.outerJoin(mr, r, on)
    case (r1, r2) => throw new UnsupportedOperationException(s"Binary relation operation for ${r1.getClass.getSimpleName} and ${r2.getClass.getSimpleName} is not yet supported!")
  }

  def innerEquiJoin[T](left: Relation, right: Relation, on: (ColumnDef[T], ColumnDef[T])): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.innerEquiJoin(tr1, tr2, on)
    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.innerEquiJoin(fr, tr, on)
    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.innerEquiJoin(mr, tr, on)
    case (fr: FutureRelation, r) => FutureRelation.BinOps.innerEquiJoin(fr, r, on)
    case (mr: MutableRelation, fr: FutureRelation) => FutureRelation.BinOps.innerEquiJoin(fr, mr, on)
    case (mr: MutableRelation, r) => MutableRelation.BinOps.innerEquiJoin(mr, r, on)
    case (r1, r2) => throw new UnsupportedOperationException(s"Binary relation operation for ${r1.getClass.getSimpleName} and ${r2.getClass.getSimpleName} is not yet supported!")
  }

  def unionAll(left: Relation, right: Relation): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.unionAll(tr1, tr2)
    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.unionAll(fr, tr)
    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.unionAll(mr, tr)
    case (fr: FutureRelation, r) => FutureRelation.BinOps.unionAll(fr, r)
    case (mr: MutableRelation, fr: FutureRelation) => FutureRelation.BinOps.unionAll(fr, mr)
    case (mr: MutableRelation, r) => MutableRelation.BinOps.unionAll(mr, r)
    case (r1, r2) => throw new UnsupportedOperationException(s"Binary relation operation for ${r1.getClass.getSimpleName} and ${r2.getClass.getSimpleName} is not yet supported!")
  }

  def union(left: Relation, right: Relation): Relation = (left, right) match {
    case (tr1: TransientRelation, tr2: TransientRelation) => TransientRelation.BinOps.union(tr1, tr2)
    case (tr: TransientRelation, fr: FutureRelation) => FutureRelation.BinOps.union(fr, tr)
    case (tr: TransientRelation, mr: MutableRelation) => MutableRelation.BinOps.union(mr, tr)
    case (fr: FutureRelation, r) => FutureRelation.BinOps.union(fr, r)
    case (mr: MutableRelation, fr: FutureRelation) => FutureRelation.BinOps.union(fr, mr)
    case (mr: MutableRelation, r) => MutableRelation.BinOps.union(mr, r)
    case (r1, r2) => throw new UnsupportedOperationException(s"Binary relation operation for ${r1.getClass.getSimpleName} and ${r2.getClass.getSimpleName} is not yet supported!")
  }
}
