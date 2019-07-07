# Meeting Minutes

16.05.2018

Participants:

- Sebastian Schmidl
- Thorsten Papenbrock
- Other Teams

## Agenda

1. Feedback and Remarks to Project Status

   - Persistence: our decision
     - we don't have to support it
     - it is taken into consideration if we have persistence
   - Actor-Model: Could be interesting to find out overhead
     - overhead of actors memory-wise
     - load lots of data into memory in a normal Java/Scala-class (naive impl.)
     - compare to loading the same data into memory distributed among actors
   - Actor-Distribution
     - physical location of actors
     - load balancing, actor migration, message routing
     - maybe: new layer between akka remoting and application dev.
     - could use own cost model (basic one: random, round-robin)
     - could provide a meta-actor-pool (akka actor-shading)
   - Access Control
     - interesting: row- or column-access-control
     - there are a lot of rules and implementing these is easy
     - granularity: what makes sense? do we even need to look at it?
     - What challenges are interesting research instead of simple implementations?

2. Mit-term presentation

   - on 11.06.2018 (alternative: 13.06.2018)
     - Thorsten will write an e-mail with details
     - with other researchers of the _Informationsystems_ chair
   - topics and content:
     - goal of our project
     - assumptions and things we intentionally left out
     - core challenge we are working on and why it is difficult
     - possible solutions
     - features, diagrams, What does our software do?
   - goal of the presentation:
     - get feedback
     - insights into possible problems and solutions

# Next Meeting

open (tbd)
