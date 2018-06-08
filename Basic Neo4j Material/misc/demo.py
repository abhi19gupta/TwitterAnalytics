# TO VIEW ALL NODES:
# MATCH (n) RETURN n

# TO CLEAR THE DATABSE:
# MATCH (n) DETACH DELETE n

from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime
import json

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
session = driver.session()


'''
USER NETWORK
    Node labels - USER(id,screen_name), USER_INFO(dict)
    Relationships - CURR_STATE(from), INTITIAL_STATE(on), PREV(from,to)

FOLLOWER NETWORK
    Node labels -
    Relationships - FOLLOWS(from,to), FOLLOWED(from,to) // FOLLOWS.to will always be the last time data was collected

TWEET NETWORK
    Node labels - TWEET(id,created_at,is_active), TWEET_INFO(dict), HASHTAG(text), URL(url,expanded_url), //MEDIA(url, media_url)//, PLACE(id,name,country) -> This is not the location of tweet but the location with which the tweet is tagged (could be about it)
    Relationships - TWEETED(on), INFO, REPLY_TO(on), RETWEET_OF(on), QUOTED(on), HAS_MENTION(on), HAS_HASHTAG(on), //HAS_MEDIA(on)//, HAS_URL(on), HAS_PLACE(on)

FRAME NETWORK
    Node labels - RUN, FRAME(start_t,end_t), TWEET_EVENT(timestamp), FOLLOW_EVENT(timestamp), UNFOLLOW_EVENT(timestamp)
    Relationships - HAS_FRAME, HAS_TWEET, TE_USER, TE_TWEET, HAS_FOLLOW, FE_FOLLOWED, FE_FOLLOWS, HAS_UNFOLLOW, UFE_UNFOLLOWED, UFE_UNFOLLOWS
'''

FRAME_DELTA_T = 5

def getFrameStartEndTime(timestamp):
    start = FRAME_DELTA_T*(timestamp//FRAME_DELTA_T)
    end = start + FRAME_DELTA_T - 1
    return (start,end)

# Modify json fields as neo4j doesn't allow nested property types i.e. only primitive
# types and their arrays are allowed to be stored as a property on a node
def flatten_json(json_obj):
    json_fields = []
    for key in json_obj:
        if type(json_obj[key]) is dict:
            json_obj[key] = json.dumps(json_obj[key])
            json_fields.append(key)
    json_obj["json_fields"] = json_fields # while fetching convert these fields back to jsons

################################################################

# CAN MAKE THE FOLLOWING CHANGE - For user_info no need of TO and FROM, just keep ON because we create a new node everytime and we have information only of that timestamp.
def update_user(id, username, user_info_dict, timestamp):
    alreadyExists = session.run(
        "MATCH (user:USER {id:{id}}) -[:CURR_STATE]-> () "
        "RETURN user",
        {"id":id})
    flatten_json(user_info_dict)
    if len(list(alreadyExists)) > 0: # if user already exists then add info to linked list
        session.run(
        # Don't do the following, it will match nothing if there is no current state
        # "MATCH (user:User {id:{user_id}}) -[curr_state:CURR_STATE]-> (prev_user_info:USER_INFO) "
        "MATCH (user:USER {id:{user_id}}) "
        "CREATE (user) -[:CURR_STATE {from:{now}}]-> (curr_user_info:USER_INFO {user_info_dict}) "
        "WITH user, curr_user_info "
        "MATCH (curr_user_info) <-[:CURR_STATE]- (user) -[prev_state_rel:CURR_STATE]-> (prev_user_info) "
        "CREATE (curr_user_info) -[:PREV {from:prev_state_rel.from, to:{now}}]-> (prev_user_info) "
        "DELETE prev_state_rel ",
        {"user_id":id, "user_info_dict":user_info_dict, "now":timestamp})
    else:
        session.run(
        "MERGE (user:USER {id:{id}}) " # not creating directly in case node already exists because of tweet network
        "ON CREATE SET user.label = {username}"
        "CREATE (user) -[:CURR_STATE {from:{now}}]-> (state:USER_INFO {user_info_dict}), "
        "  (user) -[:INITIAL_STATE {on:{now}}]-> (state) "
        "RETURN user,state",
        {"id":id, "user_info_dict":user_info_dict, "now":timestamp,"username":username})
        # for result in results:
        #   print(result["user"])

def update_followers(user_id, follower_ids, timestamp):
    (frame_start_t, frame_end_t) = getFrameStartEndTime(timestamp)
    # First, for all followers in argument, either create FOLLOWS or update the "to" field of FOLLOWS to current timestamp
    session.run(
        "MATCH (user:USER {id:{user_id}}) "
        "UNWIND {follower_ids} AS follower_id "
        "MERGE (follower:USER {id:follower_id}) " # keep this merge separate from below o/w multiple nodes can be created
        "MERGE (user) <-[follows_rel:FOLLOWS]- (follower) "
        "  ON CREATE SET follows_rel.from = {now}, follows_rel.to = {now} "
        "  ON MATCH SET follows_rel.to = {now} "
        "FOREACH (x IN CASE WHEN follows_rel.from <> {now} THEN [] ELSE [1] END | "
        "  MERGE (run:RUN) "
        "  MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
        "  CREATE (frame) -[:HAS_FOLLOW]-> (fe:FOLLOW_EVENT {timestamp:{now}}), "
        "    (fe) -[:FE_FOLLOWED]-> (user), "
        "    (fe) -[:FE_FOLLOWS]-> (follower)) ",
        {"user_id":user_id, "follower_ids":follower_ids, "now":timestamp, "frame_start_t":frame_start_t, "frame_end_t":frame_end_t})
    # Now, for all FOLLOWS whose "to" field is not current timestamp, make them FOLLOWED
    session.run(
        "MERGE (run:RUN) "
        "MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
        "WITH run, frame "
        "MATCH (user:USER {id:{user_id}}) <-[follows_rel:FOLLOWS]- (follower:USER) "
        "  WHERE follows_rel.to <> {now} "
        "CREATE (user) <-[:FOLLOWED {from:follows_rel.from, to:follows_rel.to}]- (follower), "
        "  (frame) -[:HAS_UNFOLLOW]-> (ufe:UNFOLLOW_EVENT {timestamp:{now}}), "
        "  (ufe) -[:UFE_UNFOLLOWED]-> (user), "
        "  (ufe) -[:UFE_UNFOLLOWS]-> (follower) "
        "DELETE follows_rel ", # Can change these 2 statements to a single SET statement
        {"user_id":user_id, "now":timestamp, "frame_start_t":frame_start_t, "frame_end_t":frame_end_t})

def create_tweet(tweet):

    alreadyExists = session.run(
        "MATCH (tweet:TWEET {id:{tweet_id}}) -[:INFO]-> () "
        "RETURN tweet",
        {"tweet_id":tweet["id"]})
    if len(list(alreadyExists)) > 0:
        print("create_tweet: Tweet [id:", tweet["id"], "] already exists! Aborting.")
        return

    user_id = tweet["user"]["id"]
    tweet['created_at'] = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y').timestamp()

    (frame_start_t, frame_end_t) = getFrameStartEndTime(tweet['created_at'])

    retweeted_status      = tweet.get("retweeted_status",None)
    quoted_status         = tweet.get("quoted_status",None)
    in_reply_to_status_id = tweet.get("in_reply_to_status_id",None)

    if retweeted_status is not None: # in case of retweet, it is better to rely on entities extracted from original tweet
        create_tweet(retweeted_status)
        flatten_json(tweet)
        session.run(
            # Create node for this tweet and frames
            "MERGE (run:RUN) "
            "MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
            "MERGE (user:USER {id:{user_id}}) " # Following line will make the query slow, find an alternative
            "MERGE (tweet:TWEET {id:{tweet_id}}) " # Maybe the tweet node already partially exists because of some other tweet
            "  ON CREATE SET tweet.created_at = {created_at}, tweet.is_active = true "
            "CREATE (user) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}), "
            "  (frame) -[:HAS_TWEET]-> (te:TWEET_EVENT {timestamp:{created_at}}),"
            "  (te) -[:TE_USER]-> (user),"
            "  (te) -[:TE_TWEET]-> (tweet) "
            # Find node of original tweet and link
            "WITH tweet "
            "MATCH (original_tweet:TWEET {id:{original_tweet_id}}) "
            "CREATE (tweet) -[:RETWEET_OF {on:{created_at}}]-> (original_tweet) ",
            {"user_id":user_id, "tweet_id":tweet["id"], "created_at":tweet["created_at"] ,"tweet":tweet,
            "original_tweet_id":retweeted_status["id"], "frame_start_t":frame_start_t, "frame_end_t":frame_end_t})
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
            # Create node for this tweet and frames
            "MERGE (run:RUN) "
            "MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
            "MERGE (user:USER {id:{user_id}}) " # Following line will make the query slow, find an alternative
            "MERGE (tweet:TWEET {id:{tweet_id}}) " # Maybe the tweet node already exists because of some other tweet
            "  ON CREATE SET tweet.created_at = {created_at}, tweet.is_active = true "
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
            "in_reply_to_status_id": in_reply_to_status_id, "frame_start_t":frame_start_t, "frame_end_t":frame_end_t})
        # Can remove quoted_tweet field from tweet

################################################################
def clear_db():
    session.run("MATCH (n) DETACH DELETE n")
    for index in session.run("CALL db.indexes()"):
        session.run("DROP "+index["description"])

def create_indexes():
    session.run("CREATE INDEX ON :USER(id)")
    session.run("CREATE INDEX ON :TWEET(id)")
    session.run("CREATE INDEX ON :HASHTAG(text)")
    session.run("CREATE INDEX ON :URL(url)")
    session.run("CREATE INDEX ON :FRAME(start_t)")
################################################################

def getDateFromTimestamp(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%a %b %d %H:%M:%S +0000 %Y')

def readDataAndCreateGraph(user_screen_names):
    with open("data/timestamps.txt","r") as f:
        timestamps = [x.rstrip() for x in f.readlines()]
    for time_str in timestamps:
        print("Starting for ",time_str)
        timestamp = datetime.strptime(time_str,'%Y-%m-%d %H-%M-%S.%f').timestamp()
        for screen_name in user_screen_names:
            print("\tStarting with ",screen_name)
            user_info_file = 'data/user_info/'+screen_name+"_"+time_str+'.txt'
            tweet_file     = 'data/tweets/'+screen_name+"_"+time_str+".txt"
            follower_file  = 'data/user_followers/'+screen_name+"_"+time_str+'.txt'
            friends_file   = 'data/user_friends/'+screen_name+"_"+time_str+'.txt'
            with open(user_info_file, 'r') as f:
                user_info = json.loads(f.read())
                user_id = user_info['id']
                update_user(user_id,user_info,timestamp)
                print('\t\tUser profile done')
            with open(tweet_file, 'r') as f:
                count = 0
                tweets_list_list = json.loads(f.read())
                for tweet_list in tweets_list_list:
                    for tweet in tweet_list:
                        create_tweet(tweet)
                        count += 1
                        if(count % 100 == 0):
                            print(str(count)," ")
                print('\t\tTweets done')
            with open(follower_file, 'r') as f:
                followers = json.loads(f.read())
                update_followers(user_id, followers, timestamp)
                print('\t\tFollowers done')


timestamp = 0
clear_db()
create_indexes()

start_time = datetime.now().timestamp()

tweet1 = {"id":"tweet1",
        "created_at":getDateFromTimestamp(timestamp+8),
        "details":"details1",
        "entities":{
            "hashtags":[{"text":"hash1"},{"text":"hash2"}],
            "user_mentions":[{"id":2},{"id":3}],
            "urls":[{"url":"url1","expanded_url":"eurl1"}, {"url":"url2","expanded_url":"eurl2"}]},
        "user":{"id":1}}

tweet2 = {"id":"tweet2",
        "created_at":getDateFromTimestamp(timestamp+9),
        "details":"details2",
        "entities":{
            "hashtags":[{"text":"hash1"},{"text":"hash3"}],
            "user_mentions":[{"id":2},{"id":1}],
            "urls":[{"url":"url1","expanded_url":"eurl1"}, {"url":"url3","expanded_url":"eurl3"}]},
        "user":{"id":1}}

tweet3 = {"id":"tweet3",
        "created_at":getDateFromTimestamp(timestamp+10),
        "details":"details3",
        "entities":{
            "hashtags":[],
            "user_mentions":[{"id":2}],
            "urls":[{"url":"url1","expanded_url":"eurl1"}]},
        "user":{"id":1}}

tweet4 = {"id":"tweet4",
        "created_at":getDateFromTimestamp(timestamp+11),
        "details":"details4",
        "entities":tweet3["entities"],
        "user":{"id":2},
        "retweeted_status":tweet1}

tweet5 = {"id":"tweet5",
        "created_at":getDateFromTimestamp(timestamp+15),
        "details":"details5",
        "entities":{
            "hashtags":[],
            "user_mentions":[],
            "urls":[]},
        "user":{"id":2},
        "quoted_status":tweet1,
        "in_reply_to_status_id":tweet1["id"]}


update_user(1,"Deepak",{"m1":"d1","m2":"d2"},1)
update_user(2,"Abhishek",{"m1":"d1","m2":"d2"},2)
update_user(3,"Aman",{"m1":"d1","m2":"d2"},3)
update_user(4,"Manoj",{"m1":"d1","m2":"d2"},2)

update_followers(1,[3],5)
update_followers(1,[3],8)
update_followers(1,[3,4],9)
update_followers(1,[3],13)
update_user(1,"Deepak",{"m1":"d3","m2":"d4"},timestamp+1)
update_user(1,"Deepak",{"m1":"d5","m2":"d6"},timestamp+2)

create_tweet(tweet1) # basic creation test
# create_tweet(tweet2) # testing creation of new and reuse of old
# create_tweet(tweet3) # testing empty list
create_tweet(tweet4) # testing retweet + another user (id=2)
create_tweet(tweet5) # testing quoted_status + reply

# update_user(1,{"m1":"d1","m2":"d2"},timestamp)
# update_followers(1, ["f1","f2"], timestamp+3)
# update_followers(1, ["f2","f3"], timestamp+4)
# update_followers(1, ["f1","f2"], timestamp+5)
# update_followers(1, [], timestamp+6)
# update_followers(1, ["f1","f4"], timestamp+7)


end_time = datetime.now().timestamp()
print("Time taken: ", str(end_time-start_time))

session.close()

# DEAL WITH TRUNCATION

'''
TO DISCUSS:
1. Not keeping a separate media node, because it is going to be separate for each tweet (represented by a url to the image source, so if 2 people upload same photo it will be 2 different links). Can only be reused in case of retweet in which we are anyways creating a pointer to the original tweet.
2. In case of retweet, not directly linking to hashtags etc. Instead just linking to the retweet. If you want popularity of a hashtag, you can simply count number of incoming links to this retweeted tweet.
3. Number of likes of a tweet will keep varying. Can't query all tweets now and then. Probably ignore this favorite count.
'''

'''
QUERIES
=======
All events between t1 and t2
----------------------------
WITH 1 AS t1, 14 AS t2
MATCH (run:RUN) -[:HAS_FRAME]-> (frame:FRAME)
WHERE frame.end_t >= t1 AND frame.start_t <= t2
MATCH (frame) -[]-> (event) -[]-> (actor)
WHERE event.timestamp <= t2 AND event.timestamp >= t1
RETURN frame,event,actor
'''
