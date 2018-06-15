Getting the system running
============================

Setting up the environement
-----------------------------
We recommend getting conda(or miniconda if you are low on disk space). Installing conda is easy, and the installation instructions can be found on the download page itself. After installation of conda, run the following commands:

.. code-block:: bash

    // create a new virtual environment
    conda create --name twitter_analytics python==3.6
    // clone the repo
    git clone https://github.com/abhi19gupta/TwitterAnalytics.git
    // cd into the repo
    cd TwitterAnalytics
    // activate the virtual envt
    source activate twitter_analytics
    // install the required modules.
    pip install -r requirements.txt
    .
    .
    // deactivate the virtual environment
    source deactivate

Apart from these, the user also needs to install mongoDB, neo4j databases and The Apache flink-kafka framework.

Running the dashboards
------------------------
To run the website on a local server on your machine, navigate to Dashboard Website/ and run ``python manage.py runserver``. Presently the databases will be empty, to insert the new data into the databases see the :ref:`Ingesting data into MongoDB` and the :ref:`Ingesting data into Neo4j` sections.
