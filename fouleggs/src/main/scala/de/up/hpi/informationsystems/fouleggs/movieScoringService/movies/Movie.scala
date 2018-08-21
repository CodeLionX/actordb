package de.up.hpi.informationsystems.fouleggs.movieScoringService.movies

import de.up.hpi.informationsystems.adbms.record.Record
import de.up.hpi.informationsystems.adbms.relation.Relation

case class Movie(info: Record, trailer: Relation, cast: Relation, studio: Record)
