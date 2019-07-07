# Meeting Minutes

14.06.2018

Participants:

- Thorsten
- Sebastian
- Frederic
## Agenda

1. Framework description, show and tesll

2. Memory overhead

    - Topic for thursdays intermediary presentation
    - Possible optimization: `ColumnDef`s to `RelationDef` class once, contents as array, `ColumnDef` maps to indices in said array

3. Possible research areas: *see below*

4. Additional contents for paper: *see below*

5. Futures

- Futures aren't as nice because of implicit thread creation / usage and they compete for resources with our Actors
- Alternative to this is create special Actors specifically for keeping track of ongoing requests and possibly execute transformation logic akin to what we are currently doing with `Future`s called `Function` (`adbms.Function`)
- Might be hard to generalize to get it part of the framework
- Goal: Implement for one use case in the `sampleapp` and see what we have to do repeatedly and try to generalize into framework from there

## Additional contents for paper:

- From users perspective: What does a user have to define and do to use this new paradigm and framework
- Usage pattern for our framework
- What is generalized into the framework, i.e. functionality and limitations
- Demonstrate how to realize a exemplary SQL query with our framework, (e.g. `filter` and `join`), we can also present alternative implementations and discuss tradeoffs

## Possible research areas

- Memory overhead experiments (positive feedback was given)
- How do range queries work and what is their performance especially when the data is not laid out specifically to serve those. Can we present a tradeoff if we add some logic that has to run on each asked `Dactor`
- Tradeoffs in terms of efficiency and architecture for different query patterns (example of join: each dactor joins for itself vs all is collected and one big join, i.e. join parallelization etc.)
- Comparison of different `Function` patterns for different queries / data situation / tradeoff discussion
- Idea for really big joins: ask only for join attribute, join, then ask for matching records
- Is it possible to automatically generate a working query object from declarative sql query given some restrictions, e.g. no nesting (no need to implement for now, showcase in paper)

## Framework related decisions

- Inter-`Dactor`-communication will happen using `Function`s which are state machines keeping track of `pending` requests, can chain `Request`s to various `Dactor`s, can transform results, and message final results back to the calling `Dactor`
- These can be used to implement various patterns, such as `SingleDactorFunction`, `MultiDactorFunction` (parallel requests), `PropagatingFunction`
- Start with `MultiDactorFunction`
- `Function`s are always a child actor of the calling `Dactor`, this way, if the `Function` itself fails / crashes / dies, the calling actor is notified.
- Message failure is handled by the `Function` with a given `timeout` sending back a `RequestResponseProtocol.Failure`, then the calling actor can define their own `Failure` handling
- Is it possible to unify both above failure cases using a `trait` `FunctionTerminationAsFailure` by in case of a `Function`s premature termination, pipeing the corresponding `Failure` back to oneself

## Open Questions

- In case of introducing distribution to our framework:
    - actor migration
    - hierarchy in each actor system
    - where does the knowledge about other actor systems reside/
    - in case of this being a registry, how does this look? single point of failure!
    - what happens when an entire actor system crashes?
    - which `Dactor`s go into which `ActorSystem` (load balancing, spider pattern for load diagnosis)?
    - which `Dactor`s reside together in one node?
        - dynamic actor migration depending on inter-`Dactor` message-flow
        - requires monitoring, e.g. using spider pattern, identification of connected graph structures
        - how costly is a migration of an in-memory `Dactor`? Possible cost-model?
- That being said, this is low prio for now and has explicitely been stated to not be necessary for the seminar
- Failure handling:
    - guardians for `Dactor`s? I.e. `Dactor`s always created as child actor of a guardian? Right now all `Dactor`s are direct children of the `user guardian`
    - framework or application dev
    - failure handling strategy

## ToDo's

- ongoing from last meeting (13.06.2018)

## Next meeting
