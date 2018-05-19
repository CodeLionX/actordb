# Meeting Minutes

19.05.2018

Participants:

- Sebastian Schmidl
- Frederic Schneider

## Agenda

1. Sync-up for group and supervisor feedback from 16.05.2018

    - **Persistence** is not a central aspect for the project so we will keep it **on hold** for now
    - **Access control** was deemed not too interesting as a research topic by supervisor
    - There is **interest** in the memory **overhead** of our approach as well as in how to handle actor **distribution**

2. Self-management and organisation

    - We keep focusing too much on SE related optimizations and improvements to the framework interfaces
    - From now on we will have to **focus on research project's interests**, i.e. implications of the actor model and *Akka* usage for the DB

3. Short- and Mid-term goals and respective tasks definition

    - *Short-term:* Finish the sample applications predefined message patterns / functions:
        - `add_items`
        - `checkout`
        - `get_variable_discount_update_inventory`
    - *Short-term:* `insert` message Requests for all `Relation`s of the sample application's `Dactor`s
    - *Short-term:* System startup, is a routine run from the `Main` that does all relevant *Akka* setup and possibly any setup necessary for our framework
    - *Short-term:* Test (actor) that statically creates some content for all `Dactor` extending classes in the sample application and then queries some data back to test that the messaging is working appropriately 
    - *Mid-term:* `TestDataInitializer` actor that reads data from a `csv` in some format and initializes `Dactor` contents based on the test data.

## Next Meeting

23.05.2018: We will have a short meeting no Wednesday to accomodate Sebastians busy schedule, keep each other up to date and decide on further actions if we achieved our short-term goal until then.