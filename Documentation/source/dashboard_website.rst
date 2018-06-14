Dashboard Website
=============================

Major parts of the dashboard website
--------------------------------------

We have organised the dashboard website into tabs with each tab containing associated APIs and functionality. Each tab is futher divided into sub tabs. We enlist the major tabs and subtabs and the functionality contained there in, to give an overview of the hierarchy of the website.

Hashtags
''''''''''''
This tab contains the functionality to view major statistics associated with hashtags. Though as we will see, the functionality in this tab can entirely be emulated by creating a suitable DAG, but we choose to keep a separate option to get some common stats about the common entities like hastags, user mentions and urls.

The Hashtags tab contains three subtabs:

    * Top 10 : Takes as inputs the start time and the end time, to output the 10 most popular hashtags in the said interval with the number of tweets containing the hashtag
    * Usage Plot : Takes as input a hashtag, start time and end time, to output the plot of how the usage(as number of tweets in which the hashtag occurs) of the hashtag has changed over the interval.
    * Sentiment Plot : Takes as input a hashtag, start time and end time, to output the plot of how the sentiment associated with the hashtag has changed over the interval.

Mentions
''''''''''''
The Mentions tab contains the major statistics concerning user mentions. It has the follwoing three subtabs:

    * Top 10 : Takes as inputs the start time and the end time, to output the 10 most popular users in the said interval with the number of tweets in which the user is mentioned.
    * Usage Plot : Takes as input a user, start time and end time, to output the plot of how the mention frequency(as number of tweets in which the user is mentioned) of the user has changed over the interval.
    * Sentiment Plot : Takes as input a user, start time and end time, to output the plot of how the sentiment associated with the user has changed over the interval.

URLs
'''''''''
The URLs tab contains the major statistics concerning urls. It has the follwoing three subtabs:

    * Top 10 : Takes as inputs the start time and the end time, to output the 10 most popular urls in the said interval with the number of tweets containing the url.
    * Usage Plot : Takes as input a url, start time and end time, to output the plot of how the usage(as number of tweets in which the url occurs) of the url has changed over the interval.
    * Sentiment Plot : Takes as input a url, start time and end time, to output the plot of how the sentiment associated with the url has changed over the interval.

Alerts
'''''''''

DAG
'''''
We expalin about DAGs in detail in section :ref:`Composing multiple queries : DAG`. We assume the reader has read though the section and is aware with the terminology.

This tab contains the functionalities to create and view DAGs. It has the following subtabs:

    * Create Neo4j Query : Contains the APIs to create a neo4j queries. The user provides the inputs for query cration thorugh a simple form.
    * Create MongoDB Query : Contains the APIs to create mongoDB queries.
    * Create Post-Processing Function: Contains the APIs to create a post processing function. The form contains a file upload field thorugh which the file containing the python code for the function needs to be uploaded.
    * All Queries : A color coded list of all queries created by the user, along with their input and output variables names. The user can delete queries from here.
    * Create DAG : Compose the queries seen in the list of queries to create a DAG. The structure need to be specified in a file which needs to be uploaded.
    * View DAG : Contains a list of DAGs created by the user. Also contains a button through which the user can go the airflow dashboard. Apart from that, with each DAG there is a "View" button which redirects to a page containg the strucutre code and the plotly graph of the DAG.
    * Create Custom Metric : Contains a form in which the user needs to specify a DAG and a post processing function to create metric and view it either in plot/graph format.

Use Cases
-------------
Here we walk through some major use cases of the system with snapshots to give the reader some headway on how to use the system. The system has been designed, keeping in mind that the end user may not be much proficient in computer technology and has been strucutured to be intuitive and simple. Nonetheless, the authors feel that these use case should be enough to get the user started.

Also, please notice that the easlier use cases may be used in the later ones. So it's better the reader goes through these in order.
