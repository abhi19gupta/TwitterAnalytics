Read data from Twitter API
======================================

There are a couple of ways to read data from the Twitter APIs:

   * User Timeline: Fetching the timeline (all tweets) of a given user till that time. The Twitter API end-point for this is GET statuses/user_timeline
   * Stream Sample: Streaming a random 1% sample of all live tweets. The Twitter API end-point for the same is GET statuses/sample.

If you wish to gather data of a specific set of users, then you have to use the User Timeline API, otherwise for live stream use the Stream Sample API. We experimented with both the techniques.

To have access to Twitter APIs, you need to create an app on apps.twitter.com and use their OAuth based authorization system. Details for the same can be found on this `link <https://developer.twitter.com/en/docs/basics/getting-started>`_. We made use of Python Twitter Tools library which exposes Python functions which make the API call for us based on the parameters. 


User Timeline API
------------------

For a given user, you can find the following:

   * User information: Eg. no. of tweets, no. of followers, no. of friends, no. of likes, location etc.
   * All tweets by the user till that point of time.
   * All friends and followers of that user at that point of time.

Rate Limit: Each API call has its own rate limit which limits the number of calls you can make to the Twitter API. The rate limit can be checked `here <https://developer.twitter.com/en/docs/basics/rate-limits>`_. You need to adhere to these rate limits, else your account may be blacklisted.

You may need to periodically make calls to the Twitter API to get the new tweets that were tweeted by the users since the last time you called the API. Twitter provides a mechanism to do this by using the parameter "since_id" in the statuses/user_timeline API call. The API returns tweets that have an id > since_id. Thus you can keep store of the maximum tweet id that you have seen for each user and make the next API call using that as the value of since_id to get the new tweets.

Refer to the file: 'Read Twitter Stream/main.py' for the code. The main function expects a file containing a list of user screen names separated by new lines. It generates a folder named 'data' in the same directory. Each time you run the file, it accumulates the new data in this directory.


Stream Sample API
------------------

The end-point GET statuses/sample is free, however it returns only about 1% of the actual stream. You can buy the enterprise version called Decahose to have access to 10% of the stream.

Refer to the file 'Read Twitter Stream/streaming.py' for the code. The code will write the data in the same directory, flushing the data periodically to a file. After a threshold number of tweets have been written to the current output file, it generates a new file and starts flushing the tweets to it. This will prevent a single file from becoming too big in size.

.. User Timeline API Code Documentation
.. -------------------------------------

.. Here we provide a documentation of the code.

.. .. automodule:: main
..     :members:
..     :undoc-members:
..     :inherited-members:
..     :show-inheritance:

.. Stream Sample API Code Documentation
.. -------------------------------------

.. Here we provide a documentation of the code.

.. .. automodule:: streaming
..     :members:
..     :undoc-members:
..     :inherited-members:
..     :show-inheritance: