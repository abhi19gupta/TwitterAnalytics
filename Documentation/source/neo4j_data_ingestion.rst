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
.. mention about the indexing also

.. image:: /images/neo_in1.png
.. image:: /images/neo_in2.png

Tweet network
----------------
.. image:: /images/neo_in3.png

Indexing network
-------------------
.. image:: /images/neo_in4.png

Ingesting the data into database
------------------------------------

Ingestion Rates
-----------------
.. image:: /images/neo_in5.png



Code Documentation for neo4j data ingestion
--------------------------------------------

