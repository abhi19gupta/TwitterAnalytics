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
    * Indexing network : Stores the time indexing structure utilized to answer queries having a temporal dimension. Its instructive to imagine the user and tweet network on a plane and the indexing network on top of this plane.

Let's look at these networks in some detail

User network
''''''''''''''''
The user network contains following nodes:

	* USER node : It contains user_id and screen_name.
	* USER_INFO node: It contains the user info like number of followers/friends, number of tweets, location and other meta data. These nodes form a link list in which new nodes are added, when information of the user is collected.

.. image:: /images/neo_in3.png
.. image:: /images/neo_in2.png

Tweet network
''''''''''''''''
The tweet network contains following nodes:

    * TWEET node : It contains the tweet info like its id, the raw text of the tweet, the location from which the tweet is made(currently we are storing it in raw fashion, which in future can be extended by creating LOCATION nodes just like HASHTAG et al.)
    * HASHTAG node : It contains the text of the hashtag.
    * URL node : Contains the full and the shortened url.

.. image:: /images/neo_in1.png

Indexing network
''''''''''''''''''''
The Indexing network contains:

    * RUN node : It is a unique node in the entire network. Just a starting placeholder for the root of the indexing structure.
    * FRAME node : A frame time interval is to be specified by the user. Each frame indexes time slices equal to the specified interval.
    * FOLLOW_EVENT node : represents that Follower started following Followee.
    * UNFOLLOW_EVENT node : represents that Unfollower stopped following Unfollowee.
    * TWEET_EVENT node : represents that user tweeted tweet.

.. image:: /images/neo_in4.png


Ingesting the data into neo4j : Logic
----------------------------------------

We get a tweet from the twitter firehose. One simple thing could be to make a connection to the on-disk database and insert the tweet. This can be achieved using `session.run(<cypher tweet insertion query>)`, because run in neo4j emulates a auto-commit transaction.

Improving ingestion rate using transaction
'''''''''''''''''''''''''''''''''''''''''''''
A simple way to make this more efficient would be the use of transactions. The idea behind using transactions is to keep on accumulating the incoming tweets in a graph in memory. After some fixed number of tweets, the transaction is committed to the disk. Clearly this leads to faster ingestion rate:

	* Accumulating to memory and then writing the batch to disk is faster as compared to writing tweet by tweet to disk due to the efficiency in disk head seaks.
	* Further, when creating the in-memory local graph from the transaction batch, neo4j does some changes in the order in which to write the changes to ensure efficiency.

But, the clear downside of this is that the queries being answered will lag behind at most the transaction size. This happens because the tweets are being inserted in real time manner and the queries are also being answered simultaneously. But this is not a major issue as it induces a lag of only <10 secs(assuming transaction size of ~30k).

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


Running the ingestion script
-------------------------------------------------
There are 2 applications to ingest data into Neo4j. 

Streaming data
'''''''''''''''
If the data has been collected using the Stream Sample API (as explained in :ref:`Stream Sample API`), then this application needs to be used to ingest the tweets into Neo4j. Navigate to the Ingestion/Neo4j and make changes to the file ingest_neo4j_streaming.py. Specifically, provide the folder containing the tweets containing files. We are simulating the twitter stream by reading the tweets from a file on the disk and storing those in memory. This makes sense as we can't possibly get tweets from the twitter hose at a rate greater than reading from memory, thus this in no way can be a bottleneck to the ingestion rate. Then just run the file `python ingest_neo4j_streaming.py` to start ingesting. A logs file will be created which will keep on updating to help the user gauze the ingestion rate.

User Timeline data
''''''''''''''''''
If the data has been collected using the User Timeline API (as explained in :ref:`User Timeline API`), then this application needs to be used to ingest the tweets into Neo4j. Navigate to the Ingestion/Neo4j and make changes to the file ingest_neo4j_user_timeline.py. This file ingests all the data present in the 'data' folder in the same directory. However, if the data is collected in an incremental fashion (eg. running the data collection application day by day), then this file needs to be changed accordingly to only ingest the data of the new timestamps.

Neo4j Ingestion Rates
---------------------------
.. image:: /images/neo_in5.png

Observe that the ingestion rate peaks at 1000 tweets/sec at a transaction size of around 35k. This is mainly due to the limitation of memory size. The authors observe that keeping a larger transaction size leads to lag on the system indicative of use of swap space. Thus, the maximum ingestion rate can be enhanced just by putting in more memory, albeit with decreasing returns.

Code Documentation for Neo4j data ingestion
--------------------------------------------
Here we provide a documentation of the code for ingesting tweets collected using Streaming API.

.. automodule:: ingest_neo4j_streaming
    :members:
    :undoc-members:
    :inherited-members:
    :show-inheritance:

Here we provide a documentation of the code for ingesting data collected using User Timeline API.

.. automodule:: ingest_neo4j_user_timeline
    :members:
    :undoc-members:
    :inherited-members:
    :show-inheritance:

