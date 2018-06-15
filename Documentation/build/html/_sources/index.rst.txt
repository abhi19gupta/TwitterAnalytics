.. Twitter Analytics Documentation documentation master file, created by
   sphinx-quickstart on Thu Jun  7 18:11:39 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Twitter Analytics documentation!
===========================================================

Twitter generates millions of tweets each day. A vast amount of information is hence available for different kinds of analyses. However, this also brings up three challenges. First, the system needs to be efficient enough to be able to absorb data at such high rates. Second, to be able to perform complex analysis on this data, it needs an appropriate representation in the database. Third, the system needs to provide an intuitive abstraction of the data so that the users can specify various complex analyses without having to write complex programs using different software tools. This will allow even slightly non-technical users to be able to use the system.

In this demonstration, we introduce a system that tries to tackle the above challenges. The system uses a couple of data stores - Neo4j and MongoDB - to persist the data in a way that allows complex historical queries to be answered efficiently. It also provides an intuitive abstract representation of this data, allowing users to formulate complex queries. Also, running on streaming data, the system provides an abstraction which allows the user to specify real time events in the stream, for which he wishes to be notified. We demonstrate both of these abstractions using an example of each, on real world data.

NOTE : For all experiments mentioned in this document, we use a Intel i7 processor with 16 GB RAM.

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
