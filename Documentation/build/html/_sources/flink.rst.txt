Generating alerts using Apache Flink and Kafka
==============================================

We leverage a couple of open-source technologies to detect user specified alerts in the Twitter stream. Apache Flink is an open-source, distributed and high performing stream processing tool. Apache Kafka, also open-source, is another streaming tool which can be used as a message queue for communication between programs in a highly available distributed fashion.

Tweets are continuously streamed from Twitter using the Twitter Streaming API and pushed to a Kafka topic ("tweets_topic") for downstream processes. This tweet stream is processed by Flink programs to detect the specified alerts (one Flink program per alert specification). Alert specifications are given by the user using an abstraction that we describe next.

Our alert specification abstraction is inspired from Flink's own specification. Each tweet is considered to have 3 attributes - Hashtags, URLs and User mentions. To specify an alert, user needs to specify the following:

   * Filter - values of 0 or more tweet attributes to filter the tweets relevant for the alert.
   * Group keys - 0 or more of tweet attributes on which to group and split the tweet stream (1 sub-stream per group).
   * Window length (in seconds) - to divide each sub-stream into multiple windows, each of fixed length.
   * Window slide (in seconds) - to specify how often to start a new window (windows may overlap if slide < length).
   * Count - threshold of count of tweets in any window.

As soon as the count is reached in any window, an alert is generated.

This abstraction is translated to a Flink Java application, compiled, uploaded and run by Flink server running locally. It continuously streams the tweets, processes them according to the specification and posts any alerts on a different Kafka topic ("alerts_topic"). There is a simple Python application which continuously polls for alerts on this Kafka topic and persists any found alerts to MongoDB to be displayed by the dashboard.

Example: Let us consider an example where we wish to be notified an alert if any hashtag is getting viral in the twitter stream. Suppose we define a hashtag as viral, if it is used more than 100 times in a span of 60 seconds. Now to describe this in our abstraction, we need to specify the following:

   * Filter = None; as we need to consider all tweets.
   * Group keys = Hashtag; as we need to create a sub-stream for each hashtag.
   * Window length = 60
   * Window slide = 60; say, for non-overlapping windows
   * Count = 100


.. Flink Code Documentation
.. -------------------------------------

.. Here we provide a documentation of the flink code generator.

.. .. automodule:: flink_code_gen
..     :members:
..     :undoc-members:
..     :inherited-members:
..     :show-inheritance:
