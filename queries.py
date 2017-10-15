from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime
import json
import os

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
session = driver.session()
# result  = session.run(
#     # "MATCH (u :USER) "
#     "MATCH (u:USER) "
#     "RETURN count(u) "
#     )
# for r in result:
#     print(r)
result  = session.run(
    "MATCH (u :USER) "
    "MATCH (t :TWEET) "
    "RETURN count(t),count(u) "
    )
#total events in a given time window
result1 = session.run(
    "WITH  5 AS t1, 8 AS t2 "
    "MATCH (run:RUN) -[:HAS_FRAME]-> (frame:FRAME) "
    "WHERE frame.end_t >= t1 AND frame.start_t <= t2 "
    "MATCH p = (frame) -[]-> (event) "
    "WHERE event.timestamp <= t2 AND event.timestamp >= t1 "
    "RETURN count(p)"
    )

#number of tweets in a given time window with a certain hashtag
result1 = session.run(
    "WITH  5 AS t1, 15 AS t2 "
    "MATCH (run:RUN) -[:HAS_FRAME]-> (frame:FRAME) "
    "WHERE frame.end_t >= t1 AND frame.start_t <= t2 "
    # "MATCH p = (frame) -[:HAS_TWEET]-> (event :TWEET_EVENT) -[:TE_TWEET]-> (tweet :TWEET) -[:HAS_HASHTAG]-> (hashtag :HASHTAG) "
    "MATCH q = (frame) -[:HAS_TWEET]-> (event :TWEET_EVENT) -[:TE_TWEET]-> (rt :TWEET) -[:RETWEET_OF*0..5]-> (tweet1 :TWEET) -[:HAS_HASHTAG]-> (hashtag :HASHTAG) "
    'WHERE hashtag.text = "hash1" '
    "RETURN q"
    )

for r in result1:
    print(r)
session.close()
