Neo4j: API to generate cypher queries
==========================================

Here we expalin the API to generate cypher queries for Neo4j.

Template of a general query
----------------------------------

Basic Abstraction
''''''''''''''''''''
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

Naming entities
'''''''''''''''''''

The missing thing in this abstraction is that we need to be able to distinguish that the Users in the two constraints above refer to different users. To do so, we "name" each entity (like a variable). So we have:

    * Constraint 1:
        - Source entity - u1:User, Attributes - None
        - Destination entity - u2:User, Attributes - id=1
        - Relationship - Follows, Attributes - None
    * Constraint 2:
        - Source entity - u1:User, Attributes - None
        - Destination entity - u3:Tweet, Attributes - hashtag:"h"
        - Relationship - Follows, Attributes - b/w t1,t2

Variable attributes
''''''''''''''''''''
In the example above, we considered a fixed hashtag "h". But the user can provide a variable attribute to an entity by giving it a name enclosed in curly braces `{}`. In such a case, the attribute is treated as a variable to the query and the name inside the braces is treated as the name of the said variable. For example, had we input the hashtag as `{hash1}`, it means that out query has a varibale named "hash1".


Creating a custom query through dashboard API : Behind the scenes
--------------------------------------------------------------------

A user can follow the general template of a query as provided above to build a query.
when a user provides the inputs to specify the query, the following steps are executed on the server:

    * Cleanup and processing of the inputs provided by the user.
    * The variables(User/Tweet) and the relations are stored in a database. These stored objects can be later used by the user.

Finally, to create the query the user need to specify the query name and the return variables. The query specified by the user in terms of constraints is converted into a Cypher neo4j graph mining query:

    * All variable attributes are unwinded. This connects to thw fact that we are expecting inputs as a list of native objects, hence the unwind for all inputs to the query. This ensures a cartesian cross in the query.
    * The time indexed part of the query is generated through the time indexing structure with frames and events.
    * The network part is generated through the user network and tweet network based on the relationships.
    * The return variables specified by the user are just concatenated with a return statement in the cypher.

It is to be noted here, that we don't do any kind of checking if the constraints specified by the user to build the query are valid. Checking this in a generic manner without executing the query is difficult. So, this is delegated to the neo4j query engine itself and the user will get empty result in case the constrains are invalid.

.. Code Documentation
.. ----------------------

.. Here we provide a documentation of the code.

.. .. automodule:: generate_queries
..     :members:
..     :undoc-members:
..     :inherited-members:
..     :show-inheritance:
