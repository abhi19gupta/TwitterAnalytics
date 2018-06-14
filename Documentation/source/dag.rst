Composing multiple queries : DAG
====================================

Basic terminology
---------------------

When we say **Query**, it means an one of the following three things:

    * MongoDB query : A query not capable of giving any network information
    * Neo4j query : A network based and/or time indexed query on the twitter network
    * Post processing function : A python function which takes outups of query(ies) as inputs and transforms them to give the output

**DAG** stands for directed acylic graph. Thus it a directed graph with no cycles. The idea behind a DAG is to compose mutiple queries to build a complex queries. A DAG has nodes and has directed connections connections between the nodes. Each node as a query associated with it.


Idea behind a DAG
-----------------------
As mentioned above, our main idea is to provie the user an easy abstraction to build complex queries. But apart from this there are several functions that the abstraction of a DAG seems to serve, which we list below:

    * Provide an abstraction to build complex queires from simple queries.
    * A particular database may be suited to answer particular type of queries. In fact this is the main reason behind storing data in mongoDB to answer commonly encountered queries. We expect the user to have a basic understanding of the database schemas and thus be able to have an idea of efficiency of the two databases in answering specific queries. Having such knowledge, the user can compose queries from different databases in sake of efficiency.
    * It may be easy to do some projection of data output by a query post the execution, rather than coding it in the cypher in case of neo4j, or the aggregation pipeline in case of mongoDB. Thus, given the DAG abstraction, the user can feed te output of the query into a postprocessing node.
    * On similar lines as above, the user may need to aggregate multiple outputs from different queries in a postprocessing function in a custom manner not supported by the query mechanism of the databases.
    * Breaking a big query into smaller ones may be benefitial from the end user point of view because by doing so we can show the incremental results of the smaller parts(as they are executed) to the user instead of waiting for the entire big query to execute.

In this abstraction, a single query can also be treated as a DAG, one having a single node and no connections.

We store the queries that the user creates through the dashboard. The user can then specify the structure of the DAG network by uploading a file in which he specifies how ouputs and inputs of queries are connected. We provide the details in the next section.


Building a DAG from queries
------------------------------

A DAG is composition of queries in which we need to specify how the outputs of queries upstream feed into the inputs of the downstream ones.

We explain how to build the queries with the help on an example. Let us build a DAG to get the most active users. Refer to this image(the green queries represent mongoDB queries and blue ones represent neo4j queries):

.. image:: /images/example_query.jpg

First we need to build the three queries separately, let us say we have the built queries as:

    * mongoDB query(most_popular_hashtags_20 - Node 1) - 20 most popular hashtags in total
        - INPUTS  : limit(number of records to return)
        - OUTPUTS : hashtags(list of popular hashtags, arranged by count in decreasing order), counts(list of their corresponding counts)
    * mongoDB query(most_popular_mentions_20 - Node 2) - 20 most popular users(in terms of number of mentions) in total
        - INPUTS  : limit(number of records to return)
        - OUTPUTS : user_metions(list of popular users, arranged by count in decreasing order), counts(list of their corresponding counts)
    * neo4j query(active_users - Node 3)   - userIds and their tweet counts who have used one of the popular hashtags atleast once and have tweeted with one of the popular user mentions atleast once
        - INPUTS  : hash_in(list of 20 most popular hashtags), users_in(list of 20 most popular users)
        - OUTPUTS : userIds(list of required users), tweet_counts(total number of their tweets)

This query is demonstrated by the block diagram below also:

.. image:: /images/example_query_detailed.jpg

As mentioned in neo4j query generation section, we expect all the inputs to the neo4j query to be  list of native objects. We put a similar constraint on the inputs to post processing function. Keeping this in mind, to ensure consistency and a seamless flow of information, the outputs of each query(mongoDB, neo4j or postprocessing function) is expected to be a list. Thus each node in the DAG accepts a dictionary as input in which the values are lists and similarly returns a dictionary with list values. The keys in both dictioanry is the name of the inputs/outputs, as specified in the query generation.

The only place where the list input breaks is in case of mongoDB query as they require some basic inputs which can directly be provided as native objects(for example the limit input to the above two mongoDB queries).

Further we need to specify which outputs of the queries are to be returned.

The example input file to create the above DAG looks something like this:

::

    3
    n1 most_popular_hashtags_20
    n2 most_popular_mentions_20
    n3 active_users
    INPUTS:
    CONNECTIONS:
    n1.hashtag n3.hashtag
    n2.userId n3.um_id
    RETURNS:
    n3.userId
    n3.count

Observe the struture of the file.

    * Line 1 contains the number of nodes in the DAG.
    * The following number of nodes lines contain the name of the nodes and  each node corresponds to which query.
    * Then a line contains the keyword "INPUTS:". The inputs to the queries in the DAG are specified here. For example, had the ``n1.hashtag`` variable been taken as an input rather than feefind from the outptut of an upstream query, it would have been specified as ``n1.hashtag ["hash1","hash2"]``.
    * Then a line containing the keyword "CONNECTION:". Below the connections in the DAG are specified.
    * And finally a line contains the keyword "RETURNS:". Below, we specifiy the outputs of the queries which are to be returned.

Please note that, all the outputs of all the queries can be seen in XComs in airflow and also in the logs of the DAG run. But we provide the user to specify the things of interest to the user, thourgh the RETURN variables. This will be useful in case we provide a functionality to observe the variation of a quantity over periodic DAG runs in future. Presently we don't have such a functionality in our Dashboard, though providing such a functioanlty shouldn't be difficult.

DAG in airflow
-----------------

To create a DAG in airflow, we need to create a file in the dags folder in the AIRFLOW_HOME directory. So, when the user specifies the DAG through out dashboard, we generate a python file in the metioned folder. The newly created DAG is registered with airflow after sometime(airflow has a heartbeat thread running, which looks for new DAGs in the folder periodically)
We generate the code to specify the dag in airflow something like this.

.. code-block:: python

    task_0 = PythonOperator(
        task_id='node_{}'.format("n1"),
        python_callable=execute_query,
        op_kwargs={'node_name':"n1"},
        provide_context = True,
        dag=dag)

    task_1 = PythonOperator(
            task_id='node_{}'.format("n2"),
            python_callable=execute_query,
            op_kwargs={'node_name':"n2"},
            provide_context = True,
            dag=dag)

    task_2 = PythonOperator(
            task_id='node_{}'.format("n3"),
            python_callable=execute_query,
            op_kwargs={'node_name':"n3"},
            provide_context = True,
            dag=dag)
    task_0 >> task_2
    task_1 >> task_2

In the above code, the execute query is the function in which we execute queries and pass on their outputs to XComs to be used by the downstream nodes.

.. code-block:: python

    # Pushing onto XComs
    context['task_instance'].xcom_push(k,v)
    # Pulling from XComs
    context['task_instance'].xcom_pull(task_ids=get_task_from_node(mapp[0]),dag_id = "active_users_dag",key=k)

Apart from this, some of the DAG properties which needs to be specified in airflow are generated as the default ones. For example the ``start_date`` property is specified as the current time.

Airflow provides certain other useful properties which may be of interest to the user(when the system becomes huge). For example, the user can set an email-address on which a notification will be sent in case of the success and/or failure of tasks. But, taking all these inputs though our dashboard to genereate the DAG, is like create a complete front-end wrapper over the airflow system, which is not out aim. If the user does wish to use the more involved airflow properties, he/she can always edit the source of the generated dag file. Also, if the need arises to provide the user such functionality through our dashboard, then modifying the code to generate an additional line in the dag python file is easy.

Further on airflow, different views of the DAG can be observed, some of the views which are of particular interest to us are the following :

    * Tree view - A view which tells the parallel streams in the DAG. We can specify how many parallel worker threads to have in airflow.
    * Graph view - A graph view specifying the connections between the nodes of the DAG.
    * Gant view - activates after the DAG has been executed, tells how much time taken by each query to execute.


Also,airflow provides the functionality to schedule the DAG runs periodically and properly stores the logs of each run. This can be leveraged in scenerios in which the user wants to run the same copositional query periodically.

Creating custom metric
------------------------
Custom metric can be created on top of the DAG. A custom metric is nothing but a graphical view of the data output from the DAG execution.

To view a custom metric, the user is required to specify the following things:

    * A DAG : The outputs of any queries in the DAG can be used to create the custom metric.
    * A post processing function : accepts as inputs the outputs of any of the queries in the DAG and outputs a x and y coordinates to be used for plotting.
    * Either mapping between the inputs of the post processing function and the outputs of the queries in the DAG or fixed native values to the inputs.

To display the custom metric, the DAG is executed to feed data into the post processing function. The user can choose to view the metric in either of these formats:

    * Plot : The x and y coordinates are plotted using plotly through an Ajax call and displayed on the dashboard.
    * Table : The values are displayed in table format again using an Ajax call.

An example of creating a custom metric will br provided in the :ref:`Dashboard Website` section.
