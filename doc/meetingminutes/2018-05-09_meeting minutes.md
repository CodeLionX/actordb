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
   - Keep name `project` for projecting relations and records to other schemas (like SQL _SELECT_)
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
class MyUserActor(userId: Int) extends Dactor(userId) {
  // actor name is build by Dactor
  object UserDetails extends RowRelation {
    val nameCol: ColumnDef[String] = ColumnDef("name")
    ...
    override val columnDefs = Seq(nameCol, ...)
  }
  override val relations = Map("UserDetails" -> UserDetails)

  override receive: Receive = {
    case SomeRequest(msg: String) => UserDetails.insert(UserDetails.newRecord(...))
  }
}
```
  - change `Relation` and subclasses to be used as `RowRelationDef` from the above example and get rid of the `R` val

3. Focus and Topics

   - we will not provide an SQL-like interface
   - we will be dealing with **asynchronous calls** any way
     - the interface to the outside will be implemented by the application developer as we combine application logic and data storing in one tier (Actors)
     - calls between Actors are asynchronous, but are wrapped in future-like constructs to allow synchronization for application devs.
     - we could image to create a showcase for a HTTP-Server (which is synchronous) for outbound communication
   - **Multi-table operations**
     - highly depend on the domain layout declared by the application dev.
     - again, there is the possibility to create a showcase
   - **Partitioning**
     - our concept of distributing data and application logic to domain-level actors is a type of partitioning
     - one actor contains a subset of all data of a certain relation/table, bsp:
       Customer Actor 1 contains personal details of customer 1 and their transactions,
       Customer Actor 2 contains personal details of customer 2 and their transactions, ...
   - **Access Control**
     - What is Access Control? Where will it be declared? Where is it enforced? What is the granularity?
     - `User`, `Group`, `AuthenticationActor`, access rights, access control matrix or other method?
   - maybe transactions between domain-level actors
   - outside API of our framework will be **asynchronous**
   - we will focus on **improving throughput** instead of enforcing consistency

4. TODOs:

   - (finished) Sebi: Finish `project()` functionality and tests for `Record`
   - (09.05.18) Sebi: implement better `RecordBuilder`
   - (10.05.18) Sebi: refactor `Relation` classes (see point 2)
   - (14.05.18) Sebi & Fred: `Dactor`- interface for defining domain actors
   - (09.05.18) Fred: review PRs: #13, #23
   - (10.05.18) Fred: `Seq(columnDefs)` -> `Set(columnDefs)` refactoring, think about issue: _same column name different type_
   - (10.05.18) Fred: Write all test without those for `Record` and mark `ColumnRelation` as _depricated_
   - (11.05.18) Fred: Check in `Relation` inserted `Record`s for correct columns (issue #10)

# Next Meeting

Monday, 14.05.2018
