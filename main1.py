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
	Relationships - TWEETED(on), REPLY_TO(on), RETWEET_OF(on), QUOTED(on), MENTIONS(on), HAS_HASHTAG(on), //HAS_MEDIA(on)//, HAS_URL(on), HAS_PLACE(on)
	Relationships - INFO
'''

# CAN MAKE THE FOLLOWING CHANGE - For user_info no need of TO and FROM, just keep ON because we create a new node everytime and we have information only of that timestamp.
def create_user(id, screen_name, user_info_dict, timestamp):
	alreadyExists = session.run(
		"MATCH (user:USER {id:{id}}) -[:CURR_STATE]-> () "
		"RETURN user",
		{"id":id})
	if len(list(alreadyExists)) > 0:
		print("create_user: User already exists! Aborting.")
		return
	results = session.run(
		"MERGE (user:USER {id:{id}}) " # not creating directly in case node already exists because of tweet network
		"SET user.screen_name = {screen_name} " 
		"CREATE (user) -[:CURR_STATE {from:{now}}]-> (state:USER_INFO {user_info_dict}), "
		"(user) -[:INITIAL_STATE {on:{now}}]-> (state) "
		"RETURN user,state",
		{"id":id, "screen_name":screen_name, "user_info_dict":user_info_dict, "now":timestamp})
	for result in results:
		print(result["user"])

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

def update_followers(user_id, follower_ids_str, timestamp):
	# First, for all followers in argument, either create FOLLOWS or update the "to" field of FOLLOWS to current timestamp
	session.run(
		"MATCH (user:USER {id:{user_id}}) "
		"UNWIND {follower_ids_str} AS follower_id_str "
		"MERGE (follower:USER {id_str:follower_id_str}) " # keep this merge separate from below o/w multiple nodes can be created
		"MERGE (user) <-[follows_rel:FOLLOWS]- (follower) "
		"ON CREATE SET follows_rel.from = {now}, follows_rel.to = {now} "
		"ON MATCH SET follows_rel.to = {now}",
		{"user_id":user_id, "follower_ids_str":follower_ids_str, "now":timestamp})
	# Now, for all FOLLOWS whose "to" field is not current timestamp, make them FOLLOWED
	session.run(
		"MATCH (user:USER {id:{user_id}}) <-[follows_rel:FOLLOWS]- (follower:USER) "
		"WHERE follows_rel.to <> {now} "
		"CREATE (user) <-[:FOLLOWED {from:follows_rel.from, to:follows_rel.to}]- (follower) "
		"DELETE follows_rel",
		{"user_id":user_id, "now":timestamp})

def create_tweet(user_id, tweet):
	hashtags = [x["text"] for x in tweet["entities"]["hashtags"]]
	mention_ids = [x["id"]   for x in tweet["entities"]["user_mentions"]]
	urls     = [{"url": x["url"], "expanded_url": x["expanded_url"]} for x in tweet["entities"]["urls"]]

	# Modify tweet fields as neo4j doesn't have date format and doesn't allow nested property types i.e. only primitive
	# types and their arrays are allowed to be stored as a property on a node
	tweet['created_at'] = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y').timestamp()
	json_fields = []
	for key in tweet:
		if type(tweet[key]) is dict:
			tweet[key] = json.dumps(tweet[key])
			json_fields.append(key)
	tweet["json_fields"] = json_fields # while fetching convert these fields back to jsons

	session.run(
		"MATCH (user:USER {id:{user_id}}) "
		"CREATE (user) -[:TWEETED {on:{created_at}}]-> (tweet:TWEET {id:{tweet_id},created_at:{created_at},is_active:true}) "
		"-[:INFO]-> (:TWEET_INFO {tweet}) "
		"FOREACH ( hashtag in {hashtags} | "
		"  MERGE (hashtag_node:HASHTAG {text:hashtag}) "
		"  CREATE (tweet) -[:HAS_HASHTAG {on:{created_at}}]-> (hashtag_node) ) "
		"FOREACH ( mention_id in {mention_ids} |  "
		"  MERGE (mention_node:USER {id:mention_id}) "
		"  CREATE (tweet) -[:HAS_MENTION {on:{created_at}}]-> (mention_node) ) "
		"FOREACH ( url in {urls} |  "
		"  MERGE (url_node:URL {url:url.url, expanded_url:url.expanded_url}) "
		"  CREATE (tweet) -[:HAS_URL {on:{created_at}}]-> (url_node) )",
		{"user_id":user_id, "tweet_id":tweet["id"], "created_at":tweet["created_at"] ,"tweet":tweet,
		"hashtags":hashtags, "mention_ids":mention_ids, "urls":urls})

	# isRetweet = "retweeted_status" in tweet
	# retweeted_status_id = tweet["retweeted_status"]["id"] if isRetweet else None 
	# isQuote   = "quoted_status_id" in tweet
	# quoted_status_id = tweet["quoted_status_id"] if isQuote else None
	# isReply = tweet["in_reply_to_status_id"] is not None
	# in_reply_to_status_id = tweet["in_reply_to_status_id"]


timestamp = datetime.now().timestamp()
create_user(1,"Abhishek",{"m1":"d1","m2":"d2"},timestamp)
add_user_info_to_linked_list(1,{"m1":"d3","m2":"d4"},timestamp+1)
add_user_info_to_linked_list(1,{"m1":"d5","m2":"d6"},timestamp+2)
update_followers(1, ["f1","f2"], timestamp+3)
update_followers(1, ["f2","f3"], timestamp+4)
update_followers(1, ["f1","f2"], timestamp+5)
update_followers(1, [], timestamp+6)
update_followers(1, ["f1","f4"], timestamp+7)
tweet1 = {"id":"tweet1",
		"created_at":"Sun Aug 13 15:44:16 +0000 2017",
		"details":"details1",
		"entities":{
			"hashtags":[{"text":"hash1"},{"text":"hash2"}],
			"user_mentions":[{"id":2},{"id":3}],
			"urls":[{"url":"url1","expanded_url":"eurl1"}, {"url":"url2","expanded_url":"eurl2"}]
		}}
create_tweet(1,tweet1)
tweet2 = {"id":"tweet2",
		"created_at":"Sun Aug 13 15:44:18 +0000 2017",
		"details":"details2",
		"entities":{
			"hashtags":[{"text":"hash1"},{"text":"hash3"}],
			"user_mentions":[{"id":2},{"id":1}],
			"urls":[{"url":"url1","expanded_url":"eurl1"}, {"url":"url3","expanded_url":"eurl3"}]
		}}
create_tweet(1,tweet2)
tweet3 = {"id":"tweet3",
		"created_at":"Sun Aug 13 15:44:20 +0000 2017",
		"details":"details3",
		"entities":{
			"hashtags":[],
			"user_mentions":[{"id":2}],
			"urls":[{"url":"url1","expanded_url":"eurl1"}]
		}}
create_tweet(1,tweet3)
session.close()

# TO DISCUSS: Not keeping a separate media node, because it is going to be separate for each tweet (represented by a url to the image source, so if 2 people upload same photo it will be 2 different links). Can only be reused in case of retweet in which we are anyways creating a pointer to the original tweet.