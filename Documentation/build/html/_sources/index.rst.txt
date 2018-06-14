.. Twitter Analytics Documentation documentation master file, created by
   sphinx-quickstart on Thu Jun  7 18:11:39 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Twitter Analytics documentation!
===========================================================

Twitter generates millions of tweets each day. A vast amount of information is hence available for different kinds of analyses. The end users of these analyses however, may or may not be technically proficient. This necessitates the need of a system that can absorb such large amounts of data and at the same time, provide an intuitive abstraction over this data. This abstraction should allow the end users to specify different kinds of analyses without going into the technicalities of the implementation.

In this demonstration, we introduce a system that tries to meet precisely the above needs. Running on streaming data, the system provides an abstraction which allows the user to specify real time events in the stream, for which he wishes to be notified. Also, acting as a data-store for the tweet network, the system provides another abstraction which allows the user to formulate complex queries on this historical data. We demonstrate both of these abstractions using an example of each, on real world data.

Here we provide a comprehenstive documentaion of each component of the system along with a documentation of the code.

.. toctree::
   :maxdepth: 4
   :caption: Contents:

   introduction.rst
   twitter_stream.rst
   neo4j_data_ingestion.rst
   mongoDB_data_ingestion.rst
   neo4j_query_generation.rst
   mongoDB_query_generation.rst
   postprocessing.rst
   dag.rst
   flink.rst
   benchmarking.rst
   dashboard_website.rst
   running.rst




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
