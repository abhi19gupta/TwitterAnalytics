Ingesting data into Neo4j
==============================

Data stored in neo4j
-----------------------

Our aim in the project is to capture the dynamics of an evolving social network. These dynamics can be a combination of :

    * Spatial dynamics : captured by network based information.
    * Temporal dynamics : The spatial information present at some interval of time in the past. This makes sense as the network keeps on changing and the user might want to see the state of some part of it at some point in past.

We store the complete twitter network in graph database neo4j.

For sake of understanding, lets divide the complete network into three parts:

    * User network : Stores the user information and connections between the user nodes
    * Tweet network : Stores the tweets, their attributes and interconnections between tweets and their connections with user nodes as well.
    * Indexing network : Stores the time indexing structure utilised to answer queries having a temporal dimension. Its instructive to imagine the user and tweet network on a plane and the indexing network on top of this plane.

Let's look at these networks in some detail

User network
----------------
The user network contains following nodes:

	* USER node : It contains the user info.
	* USER_INFO node: It contains the user info. These nodes forma link list in which new nodes are added, when information of the user is collected.

.. image:: /images/neo_in3.png
.. image:: /images/neo_in2.png

Tweet network
----------------
.. image:: /images/neo_in1.png

Indexing network
-------------------
.. image:: /images/neo_in4.png


Ingesting the data into neo4j : Logic
----------------------------------------

We get a tweet from the twitter firehose. One simple thing could be to make a connection to the on-disk database and insert the tweet. This can be achieved using `session.run(<cypher tweet insertion query>)`, because run in neo4j emulates a auto-commit transaction.

A simple way to make this more efficient would be the use of transactions. The idea behind using transactions is to keep on accumulating the incoming tweets in a graph in memory. After some fixed number of tweets, the transaction is commited to the disk. Clearly this leads to faster ingestion rate:

	* Accumulating to memory and then writing the batch to disk is faster as comapred to writing tweet by tweet to disk due to the efficieny in disk head seaks.
	* Further, when creating the in-memory local graph from the transaction batch, neo4j does some changes in the order in which to write the changes to ensure efficiency.

But, the clear downside of this is that the queries being answered will lag behind at most the transaction size. This happens becasue the tweets are being inserted in real time manner and the queries are also being answered simulataneoulsy. But this is not a major issue as it induces a lag of only <10 secs(assuming transaction size of ~30k).

Indexing
''''''''''
We create uniqueness constraints on the following attributes of these nodes:

+------------+------------+
|    Node    | Attribute  |
+============+============+
|    FRAME   | start_t    |
+------------+------------+
|    TWEET   |    id      |
+------------+------------+
|    USER    |    id      |
+------------+------------+
|  HASHTAG   |   text     |
+------------+------------+
|     URL    |    url     |
+------------+------------+


Ingesting the data into neo4j : Practical side
-------------------------------------------------
To ingest data into neo4j, navigate to the Ingestion/Neo4j and make changes to the file ingest_neo4j.py. Specifically, provide the folder containing the tweets containing files. We are simulating the twitter stream by reading the tweets from a file on the disk and storing those in memory. This makes sense as we can't possibly get tweets from the twitter hose at a rate greater than reading from memory, thus this in no way can be a bottleneck to the ingestion rate. Then just run the we need to run the file `python ingest_neo4j.py` to start ingesting. A logs file will be created which will keep on updating to help the user gauge the ingestion rate.

Neo4j Ingestion Rates
---------------------------
.. image:: /images/neo_in5.png

Observe that the ingestion rate peaks at 1000 tweets/sec at a transaction size of around 35k. This is mainly due to the limitation of memory size. The authors observe that keeping a larger transaction size leads to lag on the system indicative of use of swap space. Thus, the maximum ingestion rate can be enhanced just by putting in more memory, albiet with decreasing returns.

.. Code Documentation for neo4j data ingestion
.. --------------------------------------------

