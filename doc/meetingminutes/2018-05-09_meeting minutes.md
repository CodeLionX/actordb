# Meeting Minutes

09.05.2018

Participants:

- Sebastian Schmidl
- Frederic Schneider

## Agenda

1. Open Operations/Issues on `Relation` and corresponding interfaces
2. Perspective interface and name for abstract actors
3. Focus of this project and main topics of interest
4. Next TODOs with deadlines

## Decisions

1. For point 1:

   - Use `Set(columnDefs)` for defining relations and records instead of `Seq(columnDefs)` to prohibit duplicate column names.
     One issue remains unfixed: It's possible to create two `ColumnDef`s with the same name and different `valueTypes`.
   - Keep name `project`
   - Needed features:
     - Capability to create primary keys, uniqueness constraint and accessor: `get(key: Int): Record`.
       Keys are always `Int`. Primary key generation possible.
     - Tests for all public interfaces
     - Where-Condition-Builder is optional
     - improve `RecordBuilder` with better type-mismatch error messages
   - We don't allow joins on actor-internal relations
   - ResultSets of record operations are represented as `Seq[Record]`
   - `Relation` returns `Seq[Record]`, but for chaining `where`s and `project`s it will be more useful to always return a new `Relation` or a similar construct.
     We decided to keep it as it is, and postpone a decision if we need one.

2. For point 2:

  - We define one or more super classes for application Actors,
    which is responsible for all framework related tasks.
  - example use code:

```scala
class MyUserActor(userId: Int) extends TRActor(userId) {
  // actor name is build by TRActor
  object UserDetails extends RowRelationDef {
    val nameCol: ColumnDef[String] = ColumnDef("name")
    ...
    override val columnDefs = Seq(nameCol, ...)
    // val R = RowRelation(columnDefs) // inherit from RowRelationDef
  }
  override val relations = Map("UserDetails" -> UserDetails)
  
  
  override receive: Receive = {
    case SomeRequest(msg: String) => UserDetails.R.insert(R.newRecord(...))
  }
}
```
  - change `Relation` and subclasses to be used as `RowRelationDef` from the above example and get rid of the `R` val

4. TODOs:

   - Sebi: Finish `project()` functionality and tests for `Record`
   - Sebi: implement better `RecordBuilder`
   - Fred: Write all test without those for `Record`
   - Fred: Check in `Relation` inserted `Record`s for correct columns (issue #10)

# Next Meeting

Monday, 14.05.2018