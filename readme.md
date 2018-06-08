Twitter Analytics System
===============================

Introduction
--------------------

Twitter generates millions of tweets each day. A vast amount of information is hence available for different kinds of analyses. The end users of these analyses however, may or may not be technically proficient. This necessitates the need of a system that can absorb such large amounts of data and at the same time, provide an intuitive abstraction over this data. This abstraction should allow the end users to specify different kinds of analyses without going into the technicalities of the implementation.

In this demonstration, we introduce a system that tries to meet precisely the above needs. Running on streaming data, the system provides an abstraction which allows the user to specify real time events in the stream, for which he wishes to be notified. Also, acting as a data-store for the tweet network, the system provides another abstraction which allows the user to formulate complex queries on this historical data. We demonstrate both of these abstractions using an example of each, on real world data.


Directory structure of the Repo and description of files
--------------------------------------------------------------

Twitter Analytics System
===============================

Introduction
--------------------

Twitter generates millions of tweets each day. A vast amount of information is hence available for different kinds of analyses. The end users of these analyses however, may or may not be technically proficient. This necessitates the need of a system that can absorb such large amounts of data and at the same time, provide an intuitive abstraction over this data. This abstraction should allow the end users to specify different kinds of analyses without going into the technicalities of the implementation.

In this demonstration, we introduce a system that tries to meet precisely the above needs. Running on streaming data, the system provides an abstraction which allows the user to specify real time events in the stream, for which he wishes to be notified. Also, acting as a data-store for the tweet network, the system provides another abstraction which allows the user to formulate complex queries on this historical data. We demonstrate both of these abstractions using an example of each, on real world data.


Directory structure of the Repo and description of files
--------------------------------------------------------------

Here is a quick overview of the files in the repo with their brief description.
For more involved documentation, navigate to Documentation/build/html/index.html of the pdf file of the documentaion in the same directory.

```
project
│   readme.md
│   notes.txt (Some impoertant things about the tools used in the project)    
│
└───Dashboard Website (Contains the django code for the dashboard website)
│   │   manage.py
│   │   db.sqlite3
│   │
│   └───myapp (contains the code both django and logic code for dashboard functionality)
│       │   models.py
│       │   views.py
│       │   ...
|       |
|       |____airflow (contains the airflow system)
|       |____neo4j (contains the cypher query generation code for Neo4j API
|       |____mongo (contains the code to answer mongoDB queries)
|       |____dag (contains code to create DAG in networkX, do topological sort to execute the DAG, generate 
|       |             plotly div to display the dag)
|       |____flink (contains all the flinka nd kafka code)
│   
└───Benchmarking (contains all the benchmarking code to benchmark the query answering rate)
└───Basic Neo4j Material (contains the basic code form MTP1, starting new? see thsi first)
└───NLP on tweets (contains the code of different approaches to tokenize, NER of tweets, spacy seems to work best)
└───Ingestion (code to ingest data into neo4j and mongoDB, also contains code to benchmark and plot the ingestion rates)
└───Query Gen Website (old website containing just the Query Gen part, most of which incuded in the new dashboard website. Again If new to django, see this first)
```