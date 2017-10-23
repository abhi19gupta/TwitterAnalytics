# TO VIEW ALL NODES:
# MATCH (n) RETURN n

# TO CLEAR THE DATABSE:
# MATCH (n) DETACH DELETE n

from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime
import json, copy

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
session = driver.session()

INFI_TIME = 1000000000000

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
'''

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
def update_user(id, user_info_dict, timestamp):
	flatten_json(user_info_dict)
	session.run(
		"MERGE (user:USER {id:{user_id}}) WITH user " # not creating directly in case node already exists because of tweet network
		"OPTIONAL MATCH (user) -[prev_state_rel:CURR_STATE]-> (prev_user_info:USER_INFO) "
		# If prev_state_rel is null then this is a new user
		"FOREACH (x IN CASE WHEN prev_state_rel IS NULL THEN [1] ELSE [] END | "
		"  CREATE (user) -[:CURR_STATE {from:{now}}]-> (state:USER_INFO {user_info_dict}),  "
		"     (user) -[:INITIAL_STATE {on:{now}}]-> (state) ) "
		# Else this is an existing user
		"FOREACH (x IN CASE WHEN prev_state_rel IS NULL THEN [] ELSE [1] END | "
		"  CREATE (user) -[:CURR_STATE {from:{now}}]-> (:USER_INFO {user_info_dict}) "
		"    -[:PREV {from:prev_state_rel.from, to:{now}}]-> (prev_user_info) "
		"  DELETE prev_state_rel )",
		{"user_id":id, "user_info_dict":user_info_dict, "now":timestamp})

def update_followers(user_id, follower_ids, timestamp):
	# First, for all followers in argument, either create FOLLOWS or update the "to" field of FOLLOWS to current timestamp
	session.run(
		"MATCH (user:USER {id:{user_id}}) "
		"UNWIND {follower_ids} AS follower_id "
		"MERGE (follower:USER {id:follower_id}) " # keep this merge separate from below o/w multiple nodes can be created
		"MERGE (user) <-[follows_rel:FOLLOWS]- (follower) "
		"ON CREATE SET follows_rel.from = {now}, follows_rel.to = {now} "
		"ON MATCH SET follows_rel.to = {now}",
		{"user_id":user_id, "follower_ids":follower_ids, "now":timestamp})
	# Now, for all FOLLOWS whose "to" field is not current timestamp, make them FOLLOWED
	'''
	session.run(
		"MATCH (user:USER {id:{user_id}}) <-[follows_rel:FOLLOWS]- (follower:USER) "
		"WHERE follows_rel.to <> {now} "
		"CREATE (user) <-[:FOLLOWED {from:follows_rel.from, to:follows_rel.to}]- (follower) "
		"DELETE follows_rel", # Can change these 2 statements to a single SET statement
		{"user_id":user_id, "now":timestamp})
	'''

def update_friends(user_id, friend_ids, timestamp):
	# First, for all friends in argument, either create FOLLOWS or update the "to" field of FOLLOWS to current timestamp
	session.run(
		"MATCH (user:USER {id:{user_id}}) "
		"UNWIND {friend_ids} AS friend_id "
		"MERGE (friend:USER {id:friend_id}) " # keep this merge separate from below o/w multiple nodes can be created
		"MERGE (user) -[follows_rel:FOLLOWS]-> (friend) "
		"  ON CREATE SET follows_rel.from = {now}, follows_rel.to = {now} "
		"  ON MATCH SET follows_rel.to = {now} ",
		{"user_id":user_id, "friend_ids":friend_ids, "now":timestamp})
	# Now, for all FOLLOWS whose "to" field is not current timestamp, make them FOLLOWED
	'''
	session.run(
		"MATCH (user:USER {id:{user_id}}) -[follows_rel:FOLLOWS]-> (friend:USER) "
		"  WHERE follows_rel.to <> {now} "
		"CREATE (user) -[:FOLLOWED {from:follows_rel.from, to:follows_rel.to}]-> (friend) "
		"DELETE follows_rel ", # Can change these 2 statements to a single SET statement
		{"user_id":user_id, "now":timestamp})
	'''

def create_tweet(tweet):

	user_id = tweet["user"]["id"]
	tweet['created_at'] = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y').timestamp()

	retweeted_status      = tweet.get("retweeted_status",None)
	quoted_status         = tweet.get("quoted_status",None)
	in_reply_to_status_id = tweet.get("in_reply_to_status_id",None)

	if retweeted_status is not None: # in case of retweet, it is better to rely on entities extracted from original tweet
		create_tweet(retweeted_status)
		flatten_json(tweet)
		session.run(
			# Create node for this tweet
			"MERGE (tweet:TWEET {id:{tweet_id}}) " # Maybe the tweet node already partially exists because some other tweet is its reply
			"  ON CREATE SET tweet.created_at = {created_at}, tweet.is_active = true "
			"WITH tweet "
			# Proceed only if the tweet was not already created
			"MATCH (tweet) WHERE NOT (tweet) -[:INFO]-> () "
			# Create user and relationships
			"MERGE (user:USER {id:{user_id}}) " # Following line will make the query slow, find an alternative
			"CREATE (user) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}) "
			# Find node of original tweet and link
			"WITH tweet "
			"MATCH (original_tweet:TWEET {id:{original_tweet_id}}) "
			"CREATE (tweet) -[:RETWEET_OF {on:{created_at}}]-> (original_tweet) ",
			{"user_id":user_id, "tweet_id":tweet["id"], "created_at":tweet["created_at"] ,"tweet":tweet,
			"original_tweet_id":retweeted_status["id"]})
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
			# Create node for this tweet
			"MERGE (tweet:TWEET {id:{tweet_id}}) " # Maybe the tweet node already partially exists because some other tweet is its reply
			"  ON CREATE SET tweet.created_at = {created_at}, tweet.is_active = true "
			"WITH tweet "
			# Proceed only if the tweet was not already created
			"MATCH (tweet) WHERE NOT (tweet) -[:INFO]-> () "
			# Create user and relationships
			"MERGE (user:USER {id:{user_id}}) " # Following line will make the query slow, find an alternative
			"CREATE (user) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}) "
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
			"in_reply_to_status_id": in_reply_to_status_id})
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
################################################################

def getDateFromTimestamp(timestamp):
	return datetime.fromtimestamp(timestamp).strftime('%a %b %d %H:%M:%S +0000 %Y')


timestamp = 0
clear_db()
create_indexes()

start_time = datetime.now().timestamp()

timestamp = 0
print(datetime.now().timestamp())
update_user(1,{"m1":"d1","m2":"d2"},timestamp); print(datetime.now().timestamp())
update_user(1,{"m1":"d3","m2":"d4"},timestamp+1); print(datetime.now().timestamp())
update_user(1,{"m1":"d5","m2":"d6"},timestamp+2); print(datetime.now().timestamp())
update_followers(1, ["f1","f2"], timestamp+3); print(datetime.now().timestamp())
update_followers(1, ["f2","f3"], timestamp+4); print(datetime.now().timestamp())
update_followers(1, ["f1","f2"], timestamp+5); print(datetime.now().timestamp())
update_followers(1, [],          timestamp+6); print(datetime.now().timestamp())
update_followers(1, ["f1","f4"], timestamp+7); print(datetime.now().timestamp())
update_friends(1, ["g1","g2"], timestamp+3); print(datetime.now().timestamp())
update_friends(1, ["g2","g3"], timestamp+4); print(datetime.now().timestamp())
update_friends(1, ["g1","g2"], timestamp+5); print(datetime.now().timestamp())
update_friends(1, [],          timestamp+6); print(datetime.now().timestamp())
update_friends(1, ["g1","g4"], timestamp+7); print(datetime.now().timestamp())

# basic creation test
tweet1 = {"id":"tweet1",
		"created_at":getDateFromTimestamp(timestamp+8),
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

# testing retweet + another user (id=2)
tweet4 = {"id":"tweet4",
		"created_at":getDateFromTimestamp(timestamp+11),
		"details":"details4",
		"entities":tweet3["entities"],
		"user":{"id":2},
		"retweeted_status":copy.deepcopy(tweet3)}

# testing quoted_status + reply
tweet5 = {"id":"tweet5",
		"created_at":getDateFromTimestamp(timestamp+12),
		"details":"details5",
		"entities":{
			"hashtags":[],
			"user_mentions":[],
			"urls":[]},
		"user":{"id":2},
		"quoted_status":copy.deepcopy(tweet3),
		"in_reply_to_status_id":tweet1["id"]}

create_tweet(tweet1)
create_tweet(tweet2)
create_tweet(tweet3)
create_tweet(tweet4)
create_tweet(tweet5)

session.close()

end_time = datetime.now().timestamp()
print("Time taken: ", str(end_time-start_time))

# DEAL WITH TRUNCATION

'''
TO DISCUSS:
1. Not keeping a separate media node, because it is going to be separate for each tweet (represented by a url to the image source, so if 2 people upload same photo it will be 2 different links). Can only be reused in case of retweet in which we are anyways creating a pointer to the original tweet.
2. In case of retweet, not directly linking to hashtags etc. Instead just linking to the retweet. If you want popularity of a hashtag, you can simply count number of incoming links to this retweeted tweet.
3. Number of likes of a tweet will keep varying. Can't query all tweets now and then. Probably ignore this favorite count.
'''
