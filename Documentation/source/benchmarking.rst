Benchmarking the query answering
=====================================

Neo4j queries
------------------

We divide the number queries to be answered into two types. Simple  neo4j queries and complex neo4j queries. We have these queries and a set list of hashtags and userIds present in the system. We generate a list of queries to be answered by randomly picking attribute to create the query. Thus creating a single query consists of these two steps:

    * Pick a templated cypher query
    * Randomly pick the values of inputs to the query from a static list to create the query.

Similarly, we create a list of queries. The templated cypher queries are put into the simple or complex basket by seeing the time taken to answer a single query.

Having obtained the queries, we spawn multiple threads each of which opens a connection to the neo4j database in form of a session. Pops a query from the queue and delegates it to be answered by the database. To observe the optimal number of connections to be opened to the database, we plot the query answering rate verses the number of parallel connections.

Simple Queries
'''''''''''''''''

The simple queries considered are these:

    * Return count of distinct users who have used a hashtag
    * Return count of ditinct users who follow a certain user
    * Return the number of times a user was followed in a given interval
    * Find the number of current followers of users who have used certain
    * Find the count of users who tweeted in a given interval

The cypher code of these queries can also be seen here.

.. code-block:: python

    # Return count of distinct users who have used a hashtag
    q1 = """match (u:USER)-[:TWEETED]->(t:TWEET)-[:HAS_HASHTAG]->(:HASHTAG{text:"{{h}}"})
    with distinct u as u1 return count(u1)"""

    # Return count of ditinct users who follow a certain user
    q2 = """match (x:USER)-[:FOLLOWS]->(:USER {id:{{u1}}}), (x)-[:FOLLOWS]->(:USER {id:{{u2}}})
    with distinct x as x1 return count(x1)"""

    # Return the number of times a user was followed in a given interval
    q3 = """
    match (fe:FOLLOW_EVENT)-[:FE_FOLLOWED]->(u:USER {id:{{u}}})
    where fe.timestamp > {{t1}} and fe.timestamp < {{t2}}
    return count(fe)
    """

    # Find the number of current followers of users who have used certain hahstag
    q4 = """
    match (x:USER {id:{{u}}})-[:TWEETED]->(:TWEET)-[:HAS_HASHTAG]->(h:HASHTAG), (f:USER)-[:FOLLOWS]->(x), (f)-[:TWEETED]->(:TWEET)-[:HAS_HASHTAG]->(h)
    with distinct f as f1 return count(f1)
    """

    # Find the count of users who tweeted in a given interval
    q5 = """
    match (te:TWEET_EVENT)-[:TE_TWEET]->(:TWEET)-[:RETWEET_OF]->(t:TWEET), (te)-[:TE_USER]->(:USER {id:{{u}}}), (x:USER)-[:TWEETED]->(t)
    where te.timestamp < {{t1}} and te.timestamp > {{t2}}
    with distinct x as x1 return count(x1)

Using these templated queries we generate the list of simple queries to be fired as described above and observe the query ansering rate with number of parallel sessions. This graph is obtained:

.. image:: /images/simple.png


Complex Queries
'''''''''''''''''

The complex queries considered are these:

    * Return count of distinct users who have used a hashtag
    * Return count of ditinct users who follow a certain user
    * Return the number of times a user was followed in a given interval
    * Find the number of current followers of users who have used certain
    * Find the count of users who tweeted in a given interval

The cypher code of these queries can also be seen here.

.. code-block:: python

    # Return count of distinct users who have used a hashtag
    q1 = """match (u:USER)-[:TWEETED]->(t:TWEET)-[:HAS_HASHTAG]->(:HASHTAG{text:"{{h}}"})
    with distinct u as u1 return count(u1)"""

    # Return count of ditinct users who follow a certain user
    q2 = """match (x:USER)-[:FOLLOWS]->(:USER {id:{{u1}}}), (x)-[:FOLLOWS]->(:USER {id:{{u2}}})
    with distinct x as x1 return count(x1)"""

    # Return the number of times a user was followed in a given interval
    q3 = """
    match (fe:FOLLOW_EVENT)-[:FE_FOLLOWED]->(u:USER {id:{{u}}})
    where fe.timestamp > {{t1}} and fe.timestamp < {{t2}}
    return count(fe)
    """

    # Find the number of current followers of users who have used certain hahstag
    q4 = """
    match (x:USER {id:{{u}}})-[:TWEETED]->(:TWEET)-[:HAS_HASHTAG]->(h:HASHTAG), (f:USER)-[:FOLLOWS]->(x), (f)-[:TWEETED]->(:TWEET)-[:HAS_HASHTAG]->(h)
    with distinct f as f1 return count(f1)
    """

    # Find the count of users who tweeted in a given interval
    q5 = """
    match (te:TWEET_EVENT)-[:TE_TWEET]->(:TWEET)-[:RETWEET_OF]->(t:TWEET), (te)-[:TE_USER]->(:USER {id:{{u}}}), (x:USER)-[:TWEETED]->(t)
    where te.timestamp < {{t1}} and te.timestamp > {{t2}}
    with distinct x as x1 return count(x1)

Using these templated queries we generate the list of simple queries to be fired as described above and observe the query ansering rate with number of parallel sessions. This graph is obtained:

.. image:: /images/simple.png
