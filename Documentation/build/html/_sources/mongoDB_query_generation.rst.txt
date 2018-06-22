Generating queries in mongoDB
===================================

As mentioned in mongoDB ingestion section, in mongoDB we store only the data without any structural information which can be extracted quickly from incoming tweets without much processing. Further, we store in mongoDB to ensure that some very common queries can be answered quickly.

This leads to these important properties of the mongoDB part of datastore:

    * Only very specific queries can be answered using only the mongoDB. The specific queries further depend on the data which is being stored, which is further decided by which queries are seen frequently and need to be sped up. Given that currently we have the hashtag, url and user mention collection in mongoDB with the entity name, timestamp, the sentiment associated with the tweet in which the mentioned entity occurred; we can answer only specific queries like the most popular hashtag(and its sentiment) occurring in an interval(which can be the entire time as well).
    * This further means that the mongoDB schema and datastore can easily be modified and extended. For example, if I decide to store the named entities in the tweets as well, all we need is to make a new collection.

Contrast this with neo4j where the schema is more or less for fixed for all practical purposes and the user can't easily change it.

Given the above two properties, it makes little sense to develop a complete generic API to input queries from the user as it would certainly be an overkill. So currently we provide APIs only to take only specific queries from the user. The queries which can currently be answered using mongoDB are follows(the things mentioned inside <> are the inputs to the query):

    * Give the <number> most popular hashtags in total
    * Give the <number> most popular hashtags in the time interval <Begin Time> and <End Time>
    * Give the timetamps at which <hashtag> is used between <Begin Time> and <End Time>
    * Give the timetamps, associated positive and negative sentiment of a <hashtag> between <Begin Time> and <End Time>

Similarly, queries analogous to the above can also be answered for urls and user mentions.

Generic API for mongoDB : Idea
''''''''''''''''''''''''''''''''
For sake of completeness we also provide the way to generate a generic API to get mongoDB queries. For example take at this code to answer the query to get the most popular hashtags in an interval:

.. code-block:: python

    pipeline = [{"$match":{"timestamp":{"$gte":t1,"$lte":t2}}},{"$group": {"_id": "$hashtag", "count": {"$sum": 1}}},
    {"$sort": {"count":-1}},{"$limit":limit}]
    l =  self.db.ht_collection.aggregate(pipeline)["result"]
    return {"hashtag":[x["_id"] for x in l],"count":[x["count"] for x in l]}

As can be seen the query answering has three parts :

    * The aggregation pipeline: This is the part with needs to be input from the user. As we have limited number of constructs that, can be used in aggregation, Taking those as inputs along with their parameters is not that difficult. Some of the construct, though can also be filled in automatically.
    * Aggregating the collection based on the pipeline: This has fixed code and can be generated easily.
    * Unzipping the result based on the output variables name input by the user: Again, fixed code and thus easily generated.

Along with the reasons mentioned above regarding the non requirement of generic monogDB query creator, another reason is that to generate the queries, would invariably require generation of python code, something like the above snippet and then modifying a file with a new function to connect to the database and execute the python code. This would further require modifying the source code in the dashboard website and the DAG execution python functions as well to register the new function, opening several fronts from which bugs can creep in.

MongoDB query execution code documentation
--------------------------------------------
Here we provide a documentation of the code used for this functionality.

.. automodule:: execute_queries
    :members:
    :undoc-members:
    :inherited-members:
    :show-inheritance:
