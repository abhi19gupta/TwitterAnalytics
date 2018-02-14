from __future__ import print_function
from py2neo import Graph, Node, Relationship
from datetime import datetime
import json, time
from neo4j.v1 import GraphDatabase, basic_auth

FRAME_DELTA_T = 4
def getDateFromTimestamp(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%a %b %d %H:%M:%S +0000 %Y')
def getFrameStartEndTime(timestamp):
    start = FRAME_DELTA_T*(timestamp//FRAME_DELTA_T)
    end = start + FRAME_DELTA_T - 1
    return (start,end)
def flatten_json(json_obj):
    json_fields = []
    for key in json_obj:
        if type(json_obj[key]) is dict:
            json_obj[key] = json.dumps(json_obj[key])
            json_fields.append(key)
    json_obj["json_fields"] = json_fields # while fetching convert these fields back to jsons



driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
session = driver.session()

timestamp = 0
tweet1 = {"id":"tweet1",
            "created_at":getDateFromTimestamp(timestamp+1),
            "details":"details1",
            "entities":{
                "hashtags":[{"text":"hash1"},{"text":"hash2"}],
                "user_mentions":[{"id":2},{"id":3}],
                "urls":[{"url":"url1","expanded_url":"eurl1"}, {"url":"url2","expanded_url":"eurl2"}]},
            "user":{"id":1}}

# testing creation of new and reuse of old
tweet2 = {"id":"tweet2",
        "created_at":getDateFromTimestamp(timestamp+9),
        "details":"details2",
        "entities":{
            "hashtags":[{"text":"hash1"},{"text":"hash3"}],
            "user_mentions":[{"id":2},{"id":1}],
            "urls":[{"url":"url1","expanded_url":"eurl1"}, {"url":"url3","expanded_url":"eurl3"}]},
        "user":{"id":1}}

# testing empty list
tweet3 = {"id":"tweet3",
        "created_at":getDateFromTimestamp(timestamp+10),
        "details":"details3",
        "entities":{
            "hashtags":[],
            "user_mentions":[{"id":2}],
            "urls":[{"url":"url1","expanded_url":"eurl1"}]},
        "user":{"id":1}}


class Twitter:
    def __init__(self,batch_size=200,):
        self.batch_size = batch_size
        self.tweet_counter = 0
        self.graph = Graph("bolt://localhost:7687",password="password")
        self.tweet_tx = self.graph.begin()
    def clear_graph(self):
        self.graph.delete_all()
    def close(self):
        if(not(self.tweet_tx.finished())):
            print("cleaning up")
            self.tweet_tx.commit()
            self.tweet_tx = self.graph.begin()

    def create_tweet(self,tweet, favourited_by=None, fav_timestamp=None):
        print("came here")
        # print(self.graph.schema.get_uniqueness_constraints("FRAME"))
        
        self.tweet_counter+=1
        print(self.tweet_counter)
        user_id = tweet["user"]["id"]
        tweet['created_at'] = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y').timestamp()

        (frame_start_t, frame_end_t) = getFrameStartEndTime(tweet['created_at'])
        (fav_frame_start_t, fav_frame_end_t) = (None,None) if fav_timestamp is None else getFrameStartEndTime(fav_timestamp)
        
        retweeted_status      = tweet.get("retweeted_status",None)
        quoted_status         = tweet.get("quoted_status",None)
        in_reply_to_status_id = tweet.get("in_reply_to_status_id",None)

        if retweeted_status is not None: # in case of retweet, it is better to rely on entities extracted from original tweet
            self.create_tweet(retweeted_status)
            flatten_json(tweet)
            self.tweet_tx.run(
                "MERGE (run:RUN) "
                "MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
                # Create node for this tweet
                "MERGE (tweet:TWEET {id:{tweet_id}}) " # Maybe the tweet node already partially exists because some other tweet is its reply
                "  ON CREATE SET tweet.created_at = {created_at}, tweet.is_active = true "
                # Create favorite relation if needed
                "FOREACH (x IN CASE WHEN {favourited_by} IS NULL THEN [] ELSE [1] END | "
                "  MERGE (run) -[:HAS_FRAME]-> (frame_fav:FRAME {start_t:{fav_frame_start_t},end_t:{fav_frame_end_t}}) "
                "  MERGE (fav_user:USER {id:{favourited_by}}) "
                "  CREATE (fav_user)-[:LIKES {on:{fav_timestamp}}]->(tweet), "
                "    (frame_fav) -[:HAS_FAV]-> (fe:FAV_EVENT {timestamp:{fav_timestamp}}),"
                "    (fe) -[:FAV_USER]-> (fav_user),"
                "    (fe) -[:FAV_TWEET]-> (tweet) )"
                "WITH tweet "
                # Proceed only if the tweet was not already created
                "MATCH (tweet) WHERE NOT (tweet) -[:INFO]-> () "
                # Create user and then the relationships
                "MERGE (user:USER {id:{user_id}}) "
                "CREATE (user) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}), "
                "  (frame) -[:HAS_TWEET]-> (te:TWEET_EVENT {timestamp:{created_at}}),"
                "  (te) -[:TE_USER]-> (user),"
                "  (te) -[:TE_TWEET]-> (tweet) "
                # Find node of original tweet and link
                "WITH tweet "
                "MATCH (original_tweet:TWEET {id:{original_tweet_id}}) "
                "CREATE (tweet) -[:RETWEET_OF {on:{created_at}}]-> (original_tweet) ",
                {"user_id":user_id, "tweet_id":tweet["id"], "created_at":tweet["created_at"] ,"tweet":tweet,
                "original_tweet_id":retweeted_status["id"], "frame_start_t":frame_start_t, "frame_end_t":frame_end_t,
                "favourited_by":favourited_by, "fav_timestamp":fav_timestamp, "fav_frame_start_t":fav_frame_start_t,
                "fav_frame_end_t":fav_frame_end_t})
                # Can remove the retweeted_status field from tweet
        else:
            # Extract all requited information before flattening
            hashtags    = [x["text"] for x in tweet["entities"]["hashtags"]]
            mention_ids = [x["id"]   for x in tweet["entities"]["user_mentions"]]
            urls        = [{"url": x["url"], "expanded_url": x["expanded_url"]} for x in tweet["entities"]["urls"]]

            if quoted_status is not None:
                self.create_tweet(tweet["quoted_status"])

            quoted_status_id = None if quoted_status is None else quoted_status["id"]
            flatten_json(tweet)
            self.tweet_tx.run(
                "MERGE (run:RUN) "
                "MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
                # Create node for this tweet and frames
                "MERGE (tweet:TWEET {id:{tweet_id}}) " # Maybe the tweet node already partially exists because some other tweet is its reply
                "  ON CREATE SET tweet.created_at = {created_at}, tweet.is_active = true "
                # Create favorite relation if needed
                "FOREACH (x IN CASE WHEN {favourited_by} IS NULL THEN [] ELSE [1] END | "
                "  MERGE (run) -[:HAS_FRAME]-> (frame_fav:FRAME {start_t:{fav_frame_start_t},end_t:{fav_frame_end_t}}) "
                "  MERGE (fav_user:USER {id:{favourited_by}}) "
                "  CREATE (fav_user)-[:LIKES {on:{fav_timestamp}}]->(tweet), "
                "    (frame_fav) -[:HAS_FAV]-> (fe:FAV_EVENT {timestamp:{fav_timestamp}}),"
                "    (fe) -[:FAV_USER]-> (fav_user),"
                "    (fe) -[:FAV_TWEET]-> (tweet) )"
                "WITH tweet "
                # Proceed only if the tweet was not already created
                "MATCH (tweet) WHERE NOT (tweet) -[:INFO]-> () "
                # Create user and then the relationships
                "MERGE (user:USER {id:{user_id}}) "
                "CREATE (user) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}), "
                "  (frame) -[:HAS_TWEET]-> (te:TWEET_EVENT {timestamp:{created_at}}), "
                "  (te) -[:TE_USER]-> (user),"
                "  (te) -[:TE_TWEET]-> (tweet) "
                # Create links to hashtags, mentions, urls
                "FOREACH ( hashtag in {hashtags} | "
                "  MERGE (hashtag_node:HASHTAG {text:hashtag}) "
                "  CREATE (tweet) -[:HAS_HASHTAG {on:{created_at}}]-> (hashtag_node) ) "
                "FOREACH ( mention_id in {mention_ids} |  "
                "  MERGE (mention_node:USER {id:mention_id}) "
                "  CREATE (tweet) -[:HAS_MENTION {on:{created_at}}]-> (mention_node) ) "
                "FOREACH ( url in {urls} |  "
                "  MERGE (url_node:URL {url:url.url, expanded_url:url.expanded_url}) "
                "  CREATE (tweet) -[:HAS_URL {on:{created_at}}]-> (url_node) )"
                # Create link to quoted tweet in case this tweet quotes another tweet
                "FOREACH (x IN CASE WHEN {quoted_status_id} IS NULL THEN [] ELSE [1] END | "
                "  MERGE (quoted_tweet:TWEET {id:{quoted_status_id}}) "
                "  CREATE (tweet) -[:QUOTED {on:{created_at}}]-> (quoted_tweet) )"
                # Create link to original tweet in case this is a reply tweet
                "FOREACH (x IN CASE WHEN {in_reply_to_status_id} IS NULL THEN [] ELSE [1] END | "
                "  MERGE (in_reply_to_tweet:TWEET {id:{in_reply_to_status_id}}) "
                "  CREATE (tweet) -[:REPLY_TO {on:{created_at}}]-> (in_reply_to_tweet) )",
                {"user_id":user_id, "tweet_id":tweet["id"], "created_at":tweet["created_at"] ,"tweet":tweet,
                "hashtags":hashtags, "mention_ids":mention_ids, "urls":urls,
                "quoted_status":quoted_status, "quoted_status_id":quoted_status_id,
                "in_reply_to_status_id": in_reply_to_status_id, "frame_start_t":frame_start_t, "frame_end_t":frame_end_t,
                "favourited_by":favourited_by, "fav_timestamp":fav_timestamp, "fav_frame_start_t":fav_frame_start_t,
                "fav_frame_end_t":fav_frame_end_t})
        print(self.tweet_counter)
        if(self.tweet_counter>=self.batch_size):
            print("commiting the tweet_transaction")
            self.tweet_tx.commit()
            print(self.tweet_tx.finished())
            self.tweet_counter = 0
            self.tweet_tx = self.graph.begin()

def clear_db():
    session.run("MATCH (n) DETACH DELETE n")
    # for index in session.run("CALL db.indexes()"):
    #     session.run("DROP "+index["description"])
def create_tweet(tweet, favourited_by=None, fav_timestamp=None):

    # NUM_TWEETS_BUFFERED['value'] += 1
    
    user_id = tweet["user"]["id"]
    tweet['created_at'] = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y').timestamp()

    (frame_start_t, frame_end_t) = getFrameStartEndTime(tweet['created_at'])
    (fav_frame_start_t, fav_frame_end_t) = (None,None) if fav_timestamp is None else getFrameStartEndTime(fav_timestamp)
    
    retweeted_status      = tweet.get("retweeted_status",None)
    quoted_status         = tweet.get("quoted_status",None)
    in_reply_to_status_id = tweet.get("in_reply_to_status_id",None)

    if retweeted_status is not None: # in case of retweet, it is better to rely on entities extracted from original tweet
        create_tweet(retweeted_status)
        flatten_json(tweet)
        session.run(
            "MERGE (run:RUN) "
            "MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
            # Create node for this tweet
            "MERGE (tweet:TWEET {id:{tweet_id}}) " # Maybe the tweet node already partially exists because some other tweet is its reply
            "  ON CREATE SET tweet.created_at = {created_at}, tweet.is_active = true "
            # Create favorite relation if needed
            "FOREACH (x IN CASE WHEN {favourited_by} IS NULL THEN [] ELSE [1] END | "
            "  MERGE (run) -[:HAS_FRAME]-> (frame_fav:FRAME {start_t:{fav_frame_start_t},end_t:{fav_frame_end_t}}) "
            "  MERGE (fav_user:USER {id:{favourited_by}}) "
            "  CREATE (fav_user)-[:LIKES {on:{fav_timestamp}}]->(tweet), "
            "    (frame_fav) -[:HAS_FAV]-> (fe:FAV_EVENT {timestamp:{fav_timestamp}}),"
            "    (fe) -[:FAV_USER]-> (fav_user),"
            "    (fe) -[:FAV_TWEET]-> (tweet) )"
            "WITH tweet "
            # Proceed only if the tweet was not already created
            "MATCH (tweet) WHERE NOT (tweet) -[:INFO]-> () "
            # Create user and then the relationships
            "MERGE (user:USER {id:{user_id}}) "
            "CREATE (user) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}), "
            "  (frame) -[:HAS_TWEET]-> (te:TWEET_EVENT {timestamp:{created_at}}),"
            "  (te) -[:TE_USER]-> (user),"
            "  (te) -[:TE_TWEET]-> (tweet) "
            # Find node of original tweet and link
            "WITH tweet "
            "MATCH (original_tweet:TWEET {id:{original_tweet_id}}) "
            "CREATE (tweet) -[:RETWEET_OF {on:{created_at}}]-> (original_tweet) ",
            {"user_id":user_id, "tweet_id":tweet["id"], "created_at":tweet["created_at"] ,"tweet":tweet,
            "original_tweet_id":retweeted_status["id"], "frame_start_t":frame_start_t, "frame_end_t":frame_end_t,
            "favourited_by":favourited_by, "fav_timestamp":fav_timestamp, "fav_frame_start_t":fav_frame_start_t,
            "fav_frame_end_t":fav_frame_end_t})
            # Can remove the retweeted_status field from tweet
    else:
        # Extract all requited information before flattening
        hashtags    = [x["text"] for x in tweet["entities"]["hashtags"]]
        mention_ids = [x["id"]   for x in tweet["entities"]["user_mentions"]]
        urls        = [{"url": x["url"], "expanded_url": x["expanded_url"]} for x in tweet["entities"]["urls"]]

        if quoted_status is not None:
            create_tweet(tweet["quoted_status"])

        quoted_status_id = None if quoted_status is None else quoted_status["id"]
        flatten_json(tweet)
        session.run(
            "MERGE (run:RUN) "
            "MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
            # Create node for this tweet and frames
            "MERGE (tweet:TWEET {id:{tweet_id}}) " # Maybe the tweet node already partially exists because some other tweet is its reply
            "  ON CREATE SET tweet.created_at = {created_at}, tweet.is_active = true "
            # Create favorite relation if needed
            "FOREACH (x IN CASE WHEN {favourited_by} IS NULL THEN [] ELSE [1] END | "
            "  MERGE (run) -[:HAS_FRAME]-> (frame_fav:FRAME {start_t:{fav_frame_start_t},end_t:{fav_frame_end_t}}) "
            "  MERGE (fav_user:USER {id:{favourited_by}}) "
            "  CREATE (fav_user)-[:LIKES {on:{fav_timestamp}}]->(tweet), "
            "    (frame_fav) -[:HAS_FAV]-> (fe:FAV_EVENT {timestamp:{fav_timestamp}}),"
            "    (fe) -[:FAV_USER]-> (fav_user),"
            "    (fe) -[:FAV_TWEET]-> (tweet) )"
            "WITH tweet "
            # Proceed only if the tweet was not already created
            "MATCH (tweet) WHERE NOT (tweet) -[:INFO]-> () "
            # Create user and then the relationships
            "MERGE (user:USER {id:{user_id}}) "
            "CREATE (user) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}), "
            "  (frame) -[:HAS_TWEET]-> (te:TWEET_EVENT {timestamp:{created_at}}), "
            "  (te) -[:TE_USER]-> (user),"
            "  (te) -[:TE_TWEET]-> (tweet) "
            # Create links to hashtags, mentions, urls
            "FOREACH ( hashtag in {hashtags} | "
            "  MERGE (hashtag_node:HASHTAG {text:hashtag}) "
            "  CREATE (tweet) -[:HAS_HASHTAG {on:{created_at}}]-> (hashtag_node) ) "
            "FOREACH ( mention_id in {mention_ids} |  "
            "  MERGE (mention_node:USER {id:mention_id}) "
            "  CREATE (tweet) -[:HAS_MENTION {on:{created_at}}]-> (mention_node) ) "
            "FOREACH ( url in {urls} |  "
            "  MERGE (url_node:URL {url:url.url, expanded_url:url.expanded_url}) "
            "  CREATE (tweet) -[:HAS_URL {on:{created_at}}]-> (url_node) )"
            # Create link to quoted tweet in case this tweet quotes another tweet
            "FOREACH (x IN CASE WHEN {quoted_status_id} IS NULL THEN [] ELSE [1] END | "
            "  MERGE (quoted_tweet:TWEET {id:{quoted_status_id}}) "
            "  CREATE (tweet) -[:QUOTED {on:{created_at}}]-> (quoted_tweet) )"
            # Create link to original tweet in case this is a reply tweet
            "FOREACH (x IN CASE WHEN {in_reply_to_status_id} IS NULL THEN [] ELSE [1] END | "
            "  MERGE (in_reply_to_tweet:TWEET {id:{in_reply_to_status_id}}) "
            "  CREATE (tweet) -[:REPLY_TO {on:{created_at}}]-> (in_reply_to_tweet) )",
            {"user_id":user_id, "tweet_id":tweet["id"], "created_at":tweet["created_at"] ,"tweet":tweet,
            "hashtags":hashtags, "mention_ids":mention_ids, "urls":urls,
            "quoted_status":quoted_status, "quoted_status_id":quoted_status_id,
            "in_reply_to_status_id": in_reply_to_status_id, "frame_start_t":frame_start_t, "frame_end_t":frame_end_t,
            "favourited_by":favourited_by, "fav_timestamp":fav_timestamp, "fav_frame_start_t":fav_frame_start_t,
            "fav_frame_end_t":fav_frame_end_t})
        # Can remove quoted_tweet field from tweet

fin = open("data/tweets/BillGates_2017-09-17 00-05-15.191513.txt","r")
l = json.loads(fin.read())
print(len(l[0]))
# l = [[tweet1,tweet2,tweet3]]
t1 = time.time()
# create_tweets(l[0])

# clear_db()
# count = 0
# for x in l[0]:
#     count+=1
#     print("Creating tweet number ",count)
#     create_tweet(x)
# session.close()

t = Twitter()
t.clear_graph()
print("cleared the graph")
for x in l[0]:
    t.create_tweet(x)
t.close()
print("Done in time ",time.time()-t1)
