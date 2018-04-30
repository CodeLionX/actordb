# Meeting Minutes

26.04.2018

Teilnehmer:

- Sebastian Schmidl
- Frederic Schneider

## Agenda

1. Recap individual work
2. Sync understanding of _Manifesto_
   - Defines new DB type: _Actor Database System_
   - just a vision with a small prototype
   - proposes tenets and needed features
3. Decide direction of interface and the whole project
   - see below
4. Scala Resources
   - Web-Scala-REPL: https://scastie.scala-lang.org/
   - https://www.scala-exercises.org/
   - Akka-Referenz: https://doc.akka.io/docs/akka/current/index-actors.html?language=scala
4. Next tasks
   - Sebi: create doc folder structure & upload meeting notes
   - Sebi: create issue labels
   - Sebi: create Hello-World in akka
   - Sebi: intro to akka
   - Fred: intro in scala, akka
5. Next meeting
   - Montag, 30.04.2018 9:15am, on-campus

## Decisions

- application and data in one tier
- Design of an Actor Database Framework
  - like _Domain Driven Design_: splits domain/_schema_ into domain entities as actors
  - provides persistence, domain actors, ways to declare state relations and methods
  - Interface: Framework provides `Interface` for domain actors (`DomainActor`) to define relations and methods (see _Actor Database Systems: A Manifesto_), asynchronous messaging between domain actors, state is stored in child actors (`SKActor`) of those domain actors
  - example: https://scastie.scala-lang.org/CodeLionX/kSvVknaOTYiqdUPoe4nEOQ
  - first: data only in-memory, relational schemas (no KV-store)

- Example Application
  - uses Actor Database Framework
  - [Police Data Model](http://www.databaseanswers.org/data_models/police_canonical_data_model/index.htm)
