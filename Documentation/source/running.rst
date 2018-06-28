Getting the system running
============================

Setting up the environment
-----------------------------
We recommend getting conda(or miniconda if you are low on disk space). Installing conda is easy, and the installation instructions can be found on the `download page <https://conda.io/docs/user-guide/install/download.html>`_ itself. After installation of conda, run the following commands:

.. code-block:: bash

    // create a new virtual environment
    conda create --name twitter_analytics python==3.6 --file requirements_conda.txt
    // clone the repo
    git clone https://github.com/abhi19gupta/TwitterAnalytics.git
    // cd into the repo
    cd TwitterAnalytics
    // activate the virtual envt
    source activate twitter_analytics
    // install the required modules.
    pip install -r requirements_pip.txt
    .
    .
    // deactivate the virtual environment
    source deactivate

Please note that the requirements mentioned above are a superset of system's actual requirements. It may contain some ML related modules which were used while trying NER on tweets, but are not used in the final system.

The entire system has been designed and tested on Ubuntu 16.04 machine. It may work on Windows with appropriate tweaks.

Apart from these, the user also needs to install the following:

  * MongoDB: Follow the installation instructions on this `MongoDB installation link <https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/>`_.
  * Neo4j: Follow the installation instructions on this `Neo4j installation link <https://neo4j.com/docs/operations-manual/current/installation/linux/debian/>`_.
  * Apache Flink: Download the Flink 1.4.2 binary from this `Flink binary link <https://archive.apache.org/dist/flink/flink-1.4.2/>`_. If this does not work, download the latest version from the `Flink download page <https://flink.apache.org/downloads.html>`_. Once downloaded, simply extract the zip and the installation is complete. You need to use the path of this installation in the shell script as mentioned in :ref:`Running Flink and Kafka`.
  * Apache Kafka: Download the Kafka 2.11 binary from thie `Kafka binary link <https://www.apache.org/dyn/closer.cgi?path=/kafka/1.1.0/kafka_2.11-1.1.0.tgz>`_. If this does not work, download the latest version from the `Kafka download page <https://kafka.apache.org/downloads>`_. Once downloaded, simply extract the zip and the installation is complete. You need to use the path of this installation in the shell script as mentioned in :ref:`Running Flink and Kafka`.
  * Apache Airflow: Follow the installation instructions on this `Airflow installation link <https://airflow.apache.org/installation.html>`_.


Running the system
------------------------

To set the system up, the following steps need to be taken.

Collecting data
''''''''''''''''
Navigate to 'Read Twitter Stream' and run ``python streaming.py`` to collect streaming data or run ``python userstimeline.py`` to collect data for a set of users.

More details can be found here: :ref:`Read data from Twitter API`.

Ingesting data
'''''''''''''''
Before proceeding, start the Neo4j and MongoDB servers locally (using service commands in Ubuntu. Eg. ``service neo4j start``, ``service mongod start``).

Once the data is collected, you can ingest it into the databases as follows:

  * Neo4j: Navigate to Ingestion/Neo4j/ and run ``python ingest_neo4j_streaming.py`` to ingest data collected using streaming API or run ``python ingest_neo4j_user_timeline.py`` to ingest data collected using User Timeline API.
  * MongDB: In MongoDB we provide functionality to store only streaming data. Navigate to Ingestion/MongoDB and run ``python ingest_raw.py``.

More details can be found here: :ref:`Ingesting data into MongoDB` and :ref:`Ingesting data into Neo4j`

Running the dashboard
'''''''''''''''''''''' 
To run the website on a local server on your machine, navigate to Dashboard_Website/ and run ``python manage.py runserver``.

Running Flink and Kafka
''''''''''''''''''''''''
To test the Flink applications generated on the dashboard, you need to do the following:

  * Start the Kafka and Flink servers locally using the shell script in 'Kafka/' folder. (Configure the path of the Kafka and Flink directories in the shell script).
  * You can now create alert specifications on the dashboard.
  * Now, to put some tweets on the Kafka topic 'tweets_topic'. Navigate to 'Kafka/' and run ``python kafka_tweets_producer.py`` after configuring the data directory in its main function.