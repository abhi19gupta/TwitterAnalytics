Ingesting data into MongoDB
==============================

Basics First
---------------

It has no network based data, simple data just sufficient for analytical queries.
We are storing some basic statistics of the tweets in mongoDB for faster access.  Presently we are storing :

	* The number of times a hashtag, url, user mention has appeared in time intervals of 1 min.
	* The  basic  sentiment  associated  with  a  hashtag,  as  count  of  number  of  positive  and negative words

So how are we ingesting? We have two processes(most of the times running on different cores), with P1 keeping track of the statistics.  After every min, the data is put into a pipe which is received by P2 and then parallely put into mongoDB.

Ingestion Rates
-----------------

AS expected, the ingestion rate into mongoDB shilw overlapping writing into diska nd building the new batch is faster than without parallelization. This can be observed in this image:
 
.. image:: /images/image1.png
    :alt: The mongoDB ingestion rate


Code Documentation
-----------------------

.. automodule:: ingest_raw
    :members:
    :undoc-members:
    :inherited-members:
    :show-inheritance: