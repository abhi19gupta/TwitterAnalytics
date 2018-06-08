Neo4j: API to generate cypher queries
==========================================

Here we expalin the API to generate cypher queries for Neo4j.

Template of a general query
----------------------------------

Any query can be thought of as a 2 step process -

    * Extract the relevant sub-graph satisfying the query constraints (Eg. Users and their tweets that use a certain hashtag)
    * Post-processing of this sub-graph to return desired result (Eg. Return "names" of such users, Return "number" of such users)


In a generic way, the 1st step can be constructed using AND,OR,NOT of multiple constraints. We now specify how each such constraint can be built.

We look at the network in an abstract in two dimensions.

    * There are "Entities" (users and tweets) which have "Attributes" (like user has screen_name,follower_count etc. and tweet has hashtag,mentions etc.).
    * The entities have "Relations" between them which have the only attribute as time/time-interval (Eg. Follows "relation" between 2 user "entities" has a time-interval associated).


So each constraint can be specified by specifying a pattern consisting of

    * Two Entities and their Attributes
    * Relation between the entities and its Attribute (which is the time constraint of this relation)


To make things clear we provide an example here.
Suppose our query is - Find users who follow a user with id=1 and have also tweeted with a hashtag "h" between time t1 and t2.
We first break this into AND of two constraints:

    * User follows a user with id=1
    * User has tweeted with a hashtag "h" between time t1 and t2.


We now specify the 1st constraint using our entity-attribute abstraction.

    * Source entity - User, Attributes - None
    * Destination entity - User, Attributes - id=1
    * Relationship - Follows, Attributes - None

We now specify the 2nd constraint using our entity-attribute abstraction.

    * Source entity - User, Attributes - None
    * Destination entity - Tweet, Attributes - hashtag:"h"
    * Relationship - Follows, Attributes - b/w t1,t2


The missing thing in this abstraction is that we have not taken into account that the source entity in both the constraints refers to the same User. To do so, we "name" each entity (like a variable). So we have:

    * Constraint 1:
        - Source entity - u1:User, Attributes - None
        - Destination entity - u2:User, Attributes - id=1
        - Relationship - Follows, Attributes - None
    * Constraint 2:
        - Source entity - u1:User, Attributes - None
        - Destination entity - u3:Tweet, Attributes - hashtag:"h"
        - Relationship - Follows, Attributes - b/w t1,t2

Creating a custom query through dashboard API : Behind the scenes
--------------------------------------------------------------------

A user can follow the general template of a query as provided above to build a query.
when a user provides the inputs to specify the query, the following steps are executed on the server:

    * Cleanup and processing of the inputs provided by the user.
    * The variables(User/Tweet) and the relations are stored in a database. These stored objects can be later used by the user.
    * The query specified by the user is converted into a Cypher neo4j graph mining query.
    * Connection is established with the neo4j server and the query is executed on the database.
    * The results obtained are concatenated and are displayed.

Code Documentation
----------------------

Here we provide a documentation of the code.

.. automodule:: generate_queries
    :members:
    :undoc-members:
    :inherited-members:
    :show-inheritance: