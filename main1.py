# TO VIEW ALL NODES:
# MATCH (n) RETURN n

# TO CLEAR THE DATABSE:
# MATCH (n) DETACH DELETE n

from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime
import json

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

# CAN MAKE THE FOLLOWING CHANGE - For user_info no need of TO and FROM, just keep ON because we create a new node everytime and we have information only of that timestamp.
def create_user(id, screen_name, user_info_dict, timestamp):
	alreadyExists = session.run(
		"MATCH (user:USER {id:{id}}) -[:CURR_STATE]-> () "
		"RETURN user",
		{"id":id})
	if len(list(alreadyExists)) > 0:
		print("create_user: User [id:", id,"] already exists! Aborting.")
		return
	results = session.run(
		"MERGE (user:USER {id:{id}}) " # not creating directly in case node already exists because of tweet network
		"SET user.screen_name = {screen_name} " 
		"CREATE (user) -[:CURR_STATE {from:{now}}]-> (state:USER_INFO {user_info_dict}), "
		"(user) -[:INITIAL_STATE {on:{now}}]-> (state) "
		"RETURN user,state",
		{"id":id, "screen_name":screen_name, "user_info_dict":user_info_dict, "now":timestamp})
	# for result in results:
	# 	print(result["user"])

def add_user_info_to_linked_list(user_id, user_info_dict, timestamp):
	session.run(
		# Don't do the following, it will match nothing if there is no current state
		# "MATCH (user:User {id:{user_id}}) -[curr_state:CURR_STATE]-> (prev_user_info:USER_INFO) "
		"MATCH (user:USER {id:{user_id}})"
		"CREATE (user) -[:CURR_STATE {from:{now}}]-> (curr_user_info:USER_INFO {user_info_dict}) "
		"WITH user, curr_user_info "
		"MATCH (curr_user_info) <-[:CURR_STATE]- (user) -[prev_state_rel:CURR_STATE]-> (prev_user_info) "
		"CREATE (curr_user_info) -[:PREV {from:prev_state_rel.from, to:{now}}]-> (prev_user_info)"
		"DELETE prev_state_rel ",
		{"user_id":user_id, "user_info_dict":user_info_dict, "now":timestamp})

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
	session.run(
		"MATCH (user:USER {id:{user_id}}) <-[follows_rel:FOLLOWS]- (follower:USER) "
		"WHERE follows_rel.to <> {now} "
		"CREATE (user) <-[:FOLLOWED {from:follows_rel.from, to:follows_rel.to}]- (follower) "
		"DELETE follows_rel", # Can change these 2 statements to a single SET statement
		{"user_id":user_id, "now":timestamp})

def create_tweet(tweet):

	# Modify tweet fields as neo4j doesn't allow nested property types i.e. only primitive
	# types and their arrays are allowed to be stored as a property on a node
	def flatten_tweet(tweet):
		json_fields = []
		for key in tweet:
			if type(tweet[key]) is dict:
				tweet[key] = json.dumps(tweet[key])
				json_fields.append(key)
		tweet["json_fields"] = json_fields # while fetching convert these fields back to jsons


	alreadyExists = session.run(
		"MATCH (tweet:TWEET {id:{tweet_id}}) -[:INFO]-> () "
		"RETURN tweet",
		{"tweet_id":tweet["id"]})
	if len(list(alreadyExists)) > 0:
		print("create_tweet: Tweet [id:", tweet["id"], "] already exists! Aborting.")
		return
	
	user_id = tweet["user"]["id"]
	tweet['created_at'] = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y').timestamp()

	retweeted_status      = tweet.get("retweeted_status",None)
	quoted_status         = tweet.get("quoted_status",None)
	in_reply_to_status_id = tweet.get("in_reply_to_status_id",None)
	
	if retweeted_status is not None: # in case of retweet, it is better to rely on entities extracted from original tweet
		create_tweet(retweeted_status)
		flatten_tweet(tweet)
		session.run(
			# Create node for this tweet
			"MERGE (user:USER {id:{user_id}}) " # Following line will make the query slow, find an alternative
			"MERGE (tweet:TWEET {id:{tweet_id}}) " # Maybe the tweet node already partially exists because of some other tweet
			"ON CREATE SET tweet.created_at = {created_at}, tweet.is_active = true "
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
		flatten_tweet(tweet)
		session.run(
			# Create node for this tweet
			"MERGE (user:USER {id:{user_id}}) " # Following line will make the query slow, find an alternative
			"MERGE (tweet:TWEET {id:{tweet_id}}) " # Maybe the tweet node already exists because of some other tweet
			"ON CREATE SET tweet.created_at = {created_at}, tweet.is_active = true "
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

def clear_db():
	session.run("MATCH (n) DETACH DELETE n")
	for index in session.run("CALL db.indexes()"):
		session.run("DROP "+index["description"])

def create_indexes():
	session.run("CREATE INDEX ON :USER(id)")
	session.run("CREATE INDEX ON :TWEET(id)")
	session.run("CREATE INDEX ON :HAHSTAG(text)")
	session.run("CREATE INDEX ON :URL(url)")



timestamp = datetime.now().timestamp()
clear_db()
create_indexes()

start_time = datetime.now().timestamp()

print(datetime.now().timestamp())
create_user(1,"Abhishek",{"m1":"d1","m2":"d2"},timestamp)
print(datetime.now().timestamp())
create_user(10,"Abhishek",{"m1":"d1","m2":"d2"},timestamp)
print(datetime.now().timestamp())
add_user_info_to_linked_list(1,{"m1":"d3","m2":"d4"},timestamp+1)
print(datetime.now().timestamp())
add_user_info_to_linked_list(1,{"m1":"d5","m2":"d6"},timestamp+2)
print(datetime.now().timestamp())
update_followers(1, ["f1","f2"], timestamp+3)
print(datetime.now().timestamp())
update_followers(1, ["f2","f3"], timestamp+4)
print(datetime.now().timestamp())
update_followers(1, ["f1","f2"], timestamp+5)
print(datetime.now().timestamp())
update_followers(1, [], timestamp+6)
print(datetime.now().timestamp())
update_followers(1, ["f1","f4"], timestamp+7)
print(datetime.now().timestamp())

tweet1 = {"id":"tweet1",
		"created_at":"Sun Aug 13 15:44:16 +0000 2017",
		"details":"details1",
		"entities":{
			"hashtags":[{"text":"hash1"},{"text":"hash2"}],
			"user_mentions":[{"id":2},{"id":3}],
			"urls":[{"url":"url1","expanded_url":"eurl1"}, {"url":"url2","expanded_url":"eurl2"}]},
		"user":{"id":1}}
create_tweet(tweet1) # basic creation test

tweet2 = {"id":"tweet2",
		"created_at":"Sun Aug 13 15:44:18 +0000 2017",
		"details":"details2",
		"entities":{
			"hashtags":[{"text":"hash1"},{"text":"hash3"}],
			"user_mentions":[{"id":2},{"id":1}],
			"urls":[{"url":"url1","expanded_url":"eurl1"}, {"url":"url3","expanded_url":"eurl3"}]},
		"user":{"id":1}}
create_tweet(tweet2) # testing creation of new and reuse of old

tweet3 = {"id":"tweet3",
		"created_at":"Sun Aug 13 15:44:20 +0000 2017",
		"details":"details3",
		"entities":{
			"hashtags":[],
			"user_mentions":[{"id":2}],
			"urls":[{"url":"url1","expanded_url":"eurl1"}]},
		"user":{"id":1}}
# create_tweet(tweet3) # testing empty list

tweet4 = {"id":"tweet4",
		"created_at":"Sun Aug 13 15:44:22 +0000 2017",
		"details":"details4",
		"entities":tweet3["entities"],
		"user":{"id":2},
		"retweeted_status":tweet3}
create_tweet(tweet4) # testing retweet + another user (id=2)

tweet5 = {"id":"tweet5",
		"created_at":"Sun Aug 13 15:44:24 +0000 2017",
		"details":"details5",
		"entities":{
			"hashtags":[],
			"user_mentions":[],
			"urls":[]},
		"user":{"id":2},
		"quoted_status":tweet3,
		"in_reply_to_status_id":tweet1["id"]}
create_tweet(tweet5) # testing quoted_status + reply

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