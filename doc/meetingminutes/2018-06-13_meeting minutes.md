# Meeting Minutes

13.06.2018

Participants:

- Sebastian Schmidl
- Frederic Schneider

## Agenda

1. Sync-up:
  - Sebastian is working the DataInitializer and has to work around #84: `DataInitializer` cannot be mixed into direct `Dactor` subclass due to linearization issue. **Workaround:** Dactor subclass hierarchy `Dactor <- MyDactorBase <- MyDactor with DataInitialization`
  - Frederic has implemented `StoreSection` for `sampleapp`, only small changes needed to merge.
2. Next tasks:
  - Sebastian:
    - Finish PR #86
    - Data initialization in `SystemTest` using `DataInitialization` and `csv` seed data instead of hardcoded data initialization
    - [issue #62](https://github.com/CodeLionX/actordb/issues/62): Definition package refactoring
    - Benchmark data generation script
  - Frederic:
    - Finish PR #83
    - Add full functional test to `SystemTest`, i.e. provision some data, `addItems` as a Customer and then `checkout`
    - [issue #68](https://github.com/CodeLionX/actordb/issues/68): Make all successful messages return `Relation`s
    - [issue #76](https://github.com/CodeLionX/actordb/issues/76): Add test for `Dactor` companion object
    - **Memory overhead measurement related:** Add test that creates one big `Relation` for each `Relation` type and initializes its data from the same `csv` seed files as `DataInitializer` (maybe some of its functionality can even be reused in part)
    - **Memory overhead measurement related:** Load all data from `csv` seed files into memory naively e.g. simply as a `String`
    - Read resources for *Spider Pattern*
    - Read resources for `akka-cluster` and `akka-cluster-sharding` (see: [issue #84](https://github.com/CodeLionX/actordb/issues/84))
  - Roadmap:
    - See [meeting minutes from 19.05.2018](https://github.com/CodeLionX/actordb/blob/doc/meetingminutes/doc/meetingminutes/2018-05-19_meeting%20minutes.md): Interest in **memory overhead and distribution**:
      - Find appropriate profiling method to measure **memory overhead**
      - Add **distribution** (see: [issue #84](https://github.com/CodeLionX/actordb/issues/84))
3. Meeting with Thorsten
  - Describe state of our project
  - Memory overhead measurement: naive vs. `Relation` only vs. `Dactor`s - this has not been published / done by manifesto paper and could be interesting
  - **Spider Pattern** for debugging akka applications could be used for tasks like *query provisioning*, investigating data sources

## Decisions

- We are using the same seed data format for `DataInitializer` and for our benchmark test seeding
- The seed data format is comprised of directories for each `Dactor` instance containing files for each of their `Relation`s contents. We use this format even for the benchmark tests that do not instantiate any `Dactor`s - they instead add an extra column to their Relation containing the `Dactor` name corresponding to a given `Record`.

## Next meeting

**tbd** depending on meeting time with Thorsten
