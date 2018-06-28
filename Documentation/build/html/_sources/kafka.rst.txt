Putting tweets to Kafka
=======================

The tweets read from the Twitter API are written to files on the disk for any further processing. These tweets are then read from the files and put on a Kafka topic ("tweets_topic") for the downstream processes. Ideally, in a running application, the tweets from the API should directly be put on the Kafka topic without writing to the files. However, collecting data once in the files helps in easy testing of different components.

We now provide the documentation for the application that reads tweets from the files and puts them to a Kafka topic.

Kafka Tweet Producer Documentation
-------------------------------------

Here we provide a documentation of the Kafka Tweet Producer.

.. automodule:: kafka_tweets_producer
    :members:
    :undoc-members:
    :inherited-members:
    :show-inheritance: