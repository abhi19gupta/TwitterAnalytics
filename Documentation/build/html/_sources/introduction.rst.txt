Introduction to twitter analytics system
==========================================

Nowadays social media generates huge amount of data which can be used to infer a number of trending social events and to understand the connections between different entities. One such useful platform is Twitter where users can follow other users and tweet on any topic, tagging it with hashtags, mentioning other users or using URLs. This creates a complex network of entities (like users, tweets, hashtags and URLs) which are interconnected in complex ways in a temporally evolving fashion.  A number of programming tools are used by developers to answer queries or do analysis on top of this data.

However, there are a lot of people who are not so technically proficient to be able to write and maintain such a system. They generally have to rely on existing systems which either expect them to write queries in SQL (or other query languages) or offer only a partial view into the data through a limited number of available queries. Due to this reason, these people have to rely on developers to make meaning out of this data.

Our system has been designed for people with limited programming expertise. It continuously streams tweets from the Twitter Streaming API and gives users a couple of abstractions to work with. The first is an abstraction over the live stream of tweets allowing them to detect custom live events (Eg. finding hashtags going viral) in the stream and get notified about them. The second is an abstraction over the historical view over the data stored from the stream. The second abstraction looks at the data as a network of users and tweets along with their attributes.

Together, these two abstractions would provide an intuitive analytics platform for the users.


Major parts of the system
----------------------------
.. image:: /images/5.png

Lets begin by describing the major components of the system.

    * The streaming tweet collector: We have a connection to the twitter streaming API which keeps on collecting tweets from the twitter pipe. For more details refer to the section on twitter stream.
    * The alert generation system: The collected tweets are pushed to **Apache Kafka** for downstream processes. This tweet stream is processed by **Apache Flink**, which is an open-source, distributed and high-performance stream processing framework, to look for user specified alerts in the tweet stream.
    * The datastores: The stream is also persisted in a couple of data stores to make queries later. **Neo4j**, which is a graph database management system supported by the query language Cypher, is used to store network based information from these tweets. **MongoDB**, which is a document oriented database, is used to store document based information to answer simpler aggregate queries.
    * The dashboard: These alerts and queries are then accessible to the user from a web application based dashboard. Please refer to the section on dashboard website in which we explain the functionalities of the system through a running example.

