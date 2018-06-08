from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime
import json
import os
import time

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
session = driver.session()
# result  = session.run(
#     "MATCH (u :USER) "
#     "RETURN count(u) "
#     )
# for r in result:
#     print(r)
# result  = session.run(
#     "MATCH (u :USER) "
#     "MATCH (t :TWEET) "
#     "RETURN count(t),count(u) "
#     )
#total events in a given time window
# result1 = session.run(
#     "WITH  5 AS t1, 8 AS t2 "
#     "MATCH (run:RUN) -[:HAS_FRAME]-> (frame:FRAME) "
#     "WHERE frame.end_t >= t1 AND frame.start_t <= t2 "
#     "MATCH p = (frame) -[]-> (event) "
#     "WHERE event.timestamp <= t2 AND event.timestamp >= t1 "
#     "RETURN count(p)"
#     )

#number of tweets in a given time window with a certain hashtag
def match_hashtag(hashtag,t1,t2):
    result = session.run(
        "MATCH (run:RUN) -[:HAS_FRAME]-> (frame:FRAME) "
        "WHERE frame.end_t >= {t1} AND frame.start_t <= {t2} "
        "MATCH (frame) -[:HAS_TWEET]-> (event :TWEET_EVENT) -[:TE_TWEET]-> (rt :TWEET) -[:RETWEET_OF*0..1]-> (tweet :TWEET) -[:HAS_HASHTAG]-> (hashtag :HASHTAG {text:{hashtag}}) "
        'WHERE event.timestamp<={t2} AND event.timestamp>={t1} '
        "RETURN count(rt)",
        {"hashtag":hashtag,"t1":t1,"t2":t2}
    )
    return result



def match_hashtag2(hashtag,t1,t2):
    result = session.run(
        "MATCH (rt :TWEET) -[:RETWEET_OF*0..1]-> (tweet :TWEET) -[:HAS_HASHTAG]-> (hashtag :HASHTAG {text:{hashtag}}) "
        'WHERE rt.created_at<={t2} AND rt.created_at>={t1} '
        "RETURN count(rt)",
        {"hashtag":hashtag,"t1":t1,"t2":t2}
    )
    return result

def get_follow_event(t1,t2,user):
    result = session.run(
        "MATCH (run:RUN) -[:HAS_FRAME]-> (frame:FRAME) "
        "WHERE frame.end_t >= {t1} AND frame.start_t <= {t2} "
        "MATCH (frame) -[:HAS_FOLLOW]-> (event :FOLLOW_EVENT) -[FE_FOLLOWED]-> (u :USER {id:{user}}) "
        'WHERE event.timestamp<={t2} AND event.timestamp>={t1} '
        "RETURN count(event)",
        {"user":user,"t1":t1,"t2":t2}
    )
    return result

def get_follow_event2(t1,t2,user):
    result = session.run(
        "MATCH (flr) -[frel :FOLLOWS]->  (u :USER {id:{user}}) "
        'WHERE frel.from<={t2} AND frel.from>={t1} '
        "RETURN count(flr)",
        {"user":user,"t1":t1,"t2":t2}
    )
    return result

#common followers of id1 and id2
def match_common_followers(id1,id2):
    result = session.run(
    """MATCH (u1 :USER {id:{id1}}),(u2 :USER {id:{id2}}), (user :USER)
    WHERE (u1) <-[:FOLLOWS]- (user) AND (user) -[:FOLLOWS]-> (u2)
    RETURN count(user)""",
    {"id1":id1,"id2":id2}
    )
    return result

#count number of user in user_list followed by follower
def count_follows_from_list(user_list,follower):
    result  = session.run(
    "MATCH (fol :USER {id:{follower}}) -[:FOLLOWS]-> (user) "
    "WHERE user.id IN {user_list} "
    "RETURN count(user) ",
    {"user_list":user_list,"follower":follower}
    )
    return result

#find the users who have made a tweet with a certain hashtag and follow a certain user
def count_users(hashtag,followed_user):
    result = session.run(
    "MATCH (user :USER), (t :TWEET), (hashtag :HASHTAG {text:{hashtag}}) "
    "WHERE (user) -[:TWEETED]-> (t) AND (t) -[:HAS_HASHTAG]-> (hashtag)"
    "RETURN user ",
    {"hashtag":hashtag,"followed_user":followed_user}
    )
    return result

#find users who follows a id or have mentioned an id
def count_f_m(id):
    result = session.run(
    "MATCH (user :USER) -[:FOLLOWS]-> (:USER {id:{id}}) "
    "RETURN user "
    "UNION "
    "MATCH (user :USER) -[:TWEETED]-> (t :TWEET) -[:HAS_MENTION]-> (:USER {id:{id}}) "
    "RETURN user ",
    {"id":id}
        )
    return result
# bill 50393960
# modi 243023355
# musk 44196397
# srk 101311381
# kohli 71201743
# user 909451070929223681
t1 = time.time()
# result = match_hashtag("SwachhataHiSeva",1505600000,1505690000)
# result = get_follow_event(1505000000,1505900000,243023355)
# result = count_follows_from_list([50393960,243023355,44196397,101311381,71201743],909451070929223681)
# result = match_common_followers(101311381,44196397)
result = count_users("BLUERISING",71201743)
# result = count_f_m(71201743)
t2 = time.time()
print("time taken in query answer ",t2-t1)

for r in result:
    print(r)
session.close()
