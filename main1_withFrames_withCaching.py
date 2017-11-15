# TO VIEW ALL NODES:
# MATCH (n) RETURN n

# TO CLEAR THE DATABSE:
# MATCH (n) DETACH DELETE n

'''
USER NETWORK
	Node labels - USER(id), USER_INFO(dict)
	Relationships - CURR_STATE(from), INTITIAL_STATE(on), PREV(from,to)

FOLLOWER NETWORK
	Node labels -
	Relationships - FOLLOWS(from,to), FOLLOWED(from,to) // FOLLOWS.to will always be the last time data was collected

TWEET NETWORK
	Node labels - TWEET(id,created_at,is_active), TWEET_INFO(dict), HASHTAG(text), URL(url,expanded_url), //MEDIA(url, media_url)//, PLACE(id,name,country) -> This is not the location of tweet but the location with which the tweet is tagged (could be about it)
	Relationships - TWEETED(on), LIKES(on), INFO, REPLY_TO(on), RETWEET_OF(on), QUOTED(on), HAS_MENTION(on), HAS_HASHTAG(on), //HAS_MEDIA(on)//, HAS_URL(on), HAS_PLACE(on)

FRAME NETWORK
	Node labels - RUN, FRAME(start_t,end_t), TWEET_EVENT(timestamp), FOLLOW_EVENT(timestamp), UNFOLLOW_EVENT(timestamp), FAV_EVENT(timestamp)
	Relationships - HAS_FRAME, HAS_TWEET, TE_USER, TE_TWEET, HAS_FOLLOW, FE_FOLLOWED, FE_FOLLOWS, HAS_UNFOLLOW, UFE_UNFOLLOWED, UFE_UNFOLLOWS, HAS_FAV, FAV_USER, FAV_TWEET
'''

from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime
import json, copy

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
session = driver.session()

INFI_TIME = 1000000000000
FRAME_DELTA_T = 60*60*24
IS_REAL_DATA = {'value':True}
NUM_TWEETS_BUFFERED = {'value':0}
TWEET_SYNC_RATE = 25000

log_file = 'log_%s.txt'%(str(datetime.now()))
def log(text):
	print(text);
	if IS_REAL_DATA['value']:
		f = open(log_file,'a')
		f.write(text)
		f.write('\n')
		f.close()

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

def sync_session(type_=None):
	if (type_ == 'TWEET'):
		if (NUM_TWEETS_BUFFERED['value'] > TWEET_SYNC_RATE):
			sync_start_t = datetime.now().timestamp()
			session.sync()
			ret = ' Synced (%d) %f'%(NUM_TWEETS_BUFFERED['value'], datetime.now().timestamp()-sync_start_t) 
			NUM_TWEETS_BUFFERED['value'] = 0
			return ret
		else:
			return ''
	else:
		session.sync()

################################################################

class MessageQueue:
	
	def __init__(self):
		self.statements = []

	def get_queue_size(self):
		return len(self.statements)

	def add_statement(self,tweet_id, hashtags, mention_ids, urls, created_at):
		self.statements.append((tweet_id, hashtags, mention_ids, urls, created_at))

	def flush_queue(self):
		for statement in self.statements:
			session.run(
				"MATCH (tweet:TWEET {id:{tweet_id}}) "
				"FOREACH ( hashtag in {hashtags} | "
				"  MERGE (hashtag_node:HASHTAG {text:hashtag}) "
				"  CREATE (tweet) -[:HAS_HASHTAG {on:{created_at}}]-> (hashtag_node) ) "
				"FOREACH ( mention_id in {mention_ids} |  "
				"  MERGE (mention_node:USER {id:mention_id}) "
				"  CREATE (tweet) -[:HAS_MENTION {on:{created_at}}]-> (mention_node) ) "
				"FOREACH ( url in {urls} |  "
				"  MERGE (url_node:URL {url:url.url, expanded_url:url.expanded_url}) "
				"  CREATE (tweet) -[:HAS_URL {on:{created_at}}]-> (url_node) )",
				{"tweet_id":statement[0], "hashtags":statement[1], "mention_ids":statement[2], "urls":statement[3], "created_at":statement[3]})
		self.statements = []

class LRU:

	def __init__(self, capacity):
		self.capacity = capacity
		self.lru = {}
		self.hits = 0
		self.misses = 0

	def update(self, elements):
		added = []
		evicted = []
		for element in elements:
			if (element is None): continue
			inserted_el = None if element in self.lru else element
			evicted_el = None
			self.lru[element] = datetime.now().timestamp()
			if (len(self.lru) > self.capacity):
				oldest_time = INFI_TIME
				for x in self.lru:
					if (self.lru[x] < oldest_time ):
						oldest_time = self.lru[x]
						evicted_el = x
				del self.lru[evicted_el]

			if inserted_el != None: 
				self.misses += 1
				if inserted_el in evicted:
					del evicted[evicted.index(inserted_el)]
				else:
					added.append(inserted_el)
			else:
				self.hits += 1
			if evicted_el != None: 
				if evicted_el in added:
					del added[added.index(evicted_el)]
				else:
					evicted.append(evicted_el)

		return (added,evicted)

	def contains(self, element):
		return element in self.lru

	def printState(self):
		log("LRU: hits:%d misses:%d size:%d"%(self.hits,self.misses,len(self.lru)))


user_lru = LRU(capacity = 5000)
# url_lru = LRU(capacity = 5000)
hashtag_lru = LRU(capacity = 2500)



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
	(frame_start_t, frame_end_t) = getFrameStartEndTime(timestamp)
	# First, for all followers in argument, either create FOLLOWS or update the "to" field of FOLLOWS to current timestamp
	session.run(
		"MERGE (run:RUN) "
		"MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
		"WITH run, frame "
		"MATCH (user:USER {id:{user_id}}) "
		"UNWIND {follower_ids} AS follower_id "
		"MERGE (follower:USER {id:follower_id}) " # keep this merge separate from below o/w multiple nodes can be created
		"MERGE (user) <-[follows_rel:FOLLOWS]- (follower) "
		"  ON CREATE SET follows_rel.from = {now}, follows_rel.to = {now} "
		"  ON MATCH SET follows_rel.to = {now} "
		"FOREACH (x IN CASE WHEN follows_rel.from <> {now} THEN [] ELSE [1] END | "
		"  CREATE (frame) -[:HAS_FOLLOW]-> (fe:FOLLOW_EVENT {timestamp:{now}}), "
        "    (fe) -[:FE_FOLLOWED]-> (user), "
        "    (fe) -[:FE_FOLLOWS]-> (follower)) ",
		{"user_id":user_id, "follower_ids":follower_ids, "now":timestamp, "frame_start_t":frame_start_t, "frame_end_t":frame_end_t})
	# Now, for all FOLLOWS whose "to" field is not current timestamp, make them FOLLOWED
	'''
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
	'''

def update_friends(user_id, friend_ids, timestamp):
	(frame_start_t, frame_end_t) = getFrameStartEndTime(timestamp)
	# First, for all friends in argument, either create FOLLOWS or update the "to" field of FOLLOWS to current timestamp
	session.run(
		"MERGE (run:RUN) "
		"MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
		"WITH run, frame "
		"MATCH (user:USER {id:{user_id}}) "
		"UNWIND {friend_ids} AS friend_id "
		"MERGE (friend:USER {id:friend_id}) " # keep this merge separate from below o/w multiple nodes can be created
		"MERGE (user) -[follows_rel:FOLLOWS]-> (friend) "
		"  ON CREATE SET follows_rel.from = {now}, follows_rel.to = {now} "
		"  ON MATCH SET follows_rel.to = {now} "
		"FOREACH (x IN CASE WHEN follows_rel.from <> {now} THEN [] ELSE [1] END | "
		"  CREATE (frame) -[:HAS_FOLLOW]-> (fe:FOLLOW_EVENT {timestamp:{now}}), "
        "    (fe) -[:FE_FOLLOWED]-> (friend), "
        "    (fe) -[:FE_FOLLOWS]-> (user)) ",
		{"user_id":user_id, "friend_ids":friend_ids, "now":timestamp, "frame_start_t":frame_start_t, "frame_end_t":frame_end_t})
	# Now, for all FOLLOWS whose "to" field is not current timestamp, make them FOLLOWED
	'''
	session.run(
		"MERGE (run:RUN) "
		"MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
		"WITH run, frame "
		"MATCH (user:USER {id:{user_id}}) -[follows_rel:FOLLOWS]-> (friend:USER) "
		"  WHERE follows_rel.to <> {now} "
		"CREATE (user) -[:FOLLOWED {from:follows_rel.from, to:follows_rel.to}]-> (friend), "
		"  (frame) -[:HAS_UNFOLLOW]-> (ufe:UNFOLLOW_EVENT {timestamp:{now}}), "
        "  (ufe) -[:UFE_UNFOLLOWED]-> (friend), "
        "  (ufe) -[:UFE_UNFOLLOWS]-> (user) "
		"DELETE follows_rel ", # Can change these 2 statements to a single SET statement
		{"user_id":user_id, "now":timestamp, "frame_start_t":frame_start_t, "frame_end_t":frame_end_t})
	'''

def create_tweet(tweet, favourited_by=None, fav_timestamp=None):

	NUM_TWEETS_BUFFERED['value'] += 1
	
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

		(lru_add_users,lru_evict_users) = user_lru.update([favourited_by,user_id])
		fav_by_in_lru = user_lru.contains(favourited_by)
		user_in_lru = user_lru.contains(user_id)

		session.run(
			"MERGE (run:RUN) "
			"MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
			# Create node for this tweet
			"MERGE (tweet:TWEET {id:{tweet_id}}) " # Maybe the tweet node already partially exists because some other tweet is its reply
			"  ON CREATE SET tweet.created_at = {created_at}, tweet.is_active = true "
			# Update the LRU
			"FOREACH (x IN {lru_evict_users} | MERGE (u:USER_LRU {id:x}) REMOVE u:USER_LRU) "
			"FOREACH (x IN {lru_add_users} | MERGE (u:USER {id:x}) SET u:USER_LRU) "
			# Create favorite relation if needed
			"FOREACH (x IN CASE WHEN {favourited_by} IS NULL THEN [] ELSE [1] END | "
			"  MERGE (run) -[:HAS_FRAME]-> (frame_fav:FRAME {start_t:{fav_frame_start_t},end_t:{fav_frame_end_t}}) "
			"  FOREACH (y IN CASE WHEN {fav_by_in_lru} = TRUE THEN [1] ELSE [] END | "
			"    MERGE (fav_user_lru:USER_LRU {id:{favourited_by}}) "
			"    CREATE (fav_user_lru)-[:LIKES {on:{fav_timestamp}}]->(tweet), "
			"      (frame_fav) -[:HAS_FAV]-> (fe:FAV_EVENT {timestamp:{fav_timestamp}}),"
			"      (fe) -[:FAV_USER]-> (fav_user_lru),"
			"      (fe) -[:FAV_TWEET]-> (tweet) ) "
			"  FOREACH (y IN CASE WHEN {fav_by_in_lru} = FALSE THEN [1] ELSE [] END | "
			"    MERGE (fav_user:USER {id:{favourited_by}}) "
			"    CREATE (fav_user)-[:LIKES {on:{fav_timestamp}}]->(tweet), "
			"      (frame_fav) -[:HAS_FAV]-> (fe:FAV_EVENT {timestamp:{fav_timestamp}}),"
			"      (fe) -[:FAV_USER]-> (fav_user),"
			"      (fe) -[:FAV_TWEET]-> (tweet) ) "
			") "
			"WITH tweet "
			# Proceed only if the tweet was not already created
			"MATCH (tweet) WHERE NOT (tweet) -[:INFO]-> () "
			# Create user and then the relationships
			"FOREACH (x IN CASE WHEN {user_in_lru} = TRUE THEN [1] ELSE [] END | "
			"  MERGE (user_lru:USER_LRU {id:{user_id}}) "
			"  CREATE (user_lru) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}), "
			"    (frame) -[:HAS_TWEET]-> (te:TWEET_EVENT {timestamp:{created_at}}),"
			"    (te) -[:TE_USER]-> (user_lru),"
			"    (te) -[:TE_TWEET]-> (tweet) ) "
			"FOREACH (x IN CASE WHEN {user_in_lru} = FALSE THEN [1] ELSE [] END | "
			"  MERGE (user:USER {id:{user_id}}) "
			"  CREATE (user) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}), "
			"    (frame) -[:HAS_TWEET]-> (te:TWEET_EVENT {timestamp:{created_at}}),"
			"    (te) -[:TE_USER]-> (user),"
			"    (te) -[:TE_TWEET]-> (tweet) ) "
			# Find node of original tweet and link
			"WITH tweet "
			"MATCH (original_tweet:TWEET {id:{original_tweet_id}}) "
			"CREATE (tweet) -[:RETWEET_OF {on:{created_at}}]-> (original_tweet) ",
			{"user_id":user_id, "tweet_id":tweet["id"], "created_at":tweet["created_at"] ,"tweet":tweet,
			"original_tweet_id":retweeted_status["id"], "frame_start_t":frame_start_t, "frame_end_t":frame_end_t,
			"favourited_by":favourited_by, "fav_timestamp":fav_timestamp, "fav_frame_start_t":fav_frame_start_t,
			"fav_frame_end_t":fav_frame_end_t, "fav_by_in_lru":fav_by_in_lru, "user_in_lru":user_in_lru, 
			"lru_evict_users":lru_evict_users, "lru_add_users":lru_add_users})
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

		(lru_add_users,lru_evict_users) = user_lru.update([favourited_by, user_id]+mention_ids)
		fav_by_in_lru = user_lru.contains(favourited_by)
		user_in_lru = user_lru.contains(user_id)
		mention_ids_lru = []
		mention_ids_non_lru = []
		for mention_id in mention_ids:
			if (user_lru.contains(mention_id)): 
				mention_ids_lru.append(mention_id)
			else:
				mention_ids_non_lru.append(mention_id)

		session.run(
			"MERGE (run:RUN) "
			"MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
			# Create node for this tweet and frames
			"MERGE (tweet:TWEET {id:{tweet_id}}) " # Maybe the tweet node already partially exists because some other tweet is its reply
			"  ON CREATE SET tweet.created_at = {created_at}, tweet.is_active = true "
			# Update the LRU
			"FOREACH (x IN {lru_evict_users} | MERGE (u:USER_LRU {id:x}) REMOVE u:USER_LRU) "
			"FOREACH (x IN {lru_add_users} | MERGE (u:USER {id:x}) SET u:USER_LRU) "
			# Create favorite relation if needed
			"FOREACH (x IN CASE WHEN {favourited_by} IS NULL THEN [] ELSE [1] END | "
			"  MERGE (run) -[:HAS_FRAME]-> (frame_fav:FRAME {start_t:{fav_frame_start_t},end_t:{fav_frame_end_t}}) "
			"  FOREACH (y IN CASE WHEN {fav_by_in_lru} = TRUE THEN [1] ELSE [] END | "
			"    MERGE (fav_user_lru:USER_LRU {id:{favourited_by}}) "
			"    CREATE (fav_user_lru)-[:LIKES {on:{fav_timestamp}}]->(tweet), "
			"      (frame_fav) -[:HAS_FAV]-> (fe:FAV_EVENT {timestamp:{fav_timestamp}}),"
			"      (fe) -[:FAV_USER]-> (fav_user_lru),"
			"      (fe) -[:FAV_TWEET]-> (tweet) ) "
			"  FOREACH (y IN CASE WHEN {fav_by_in_lru} = FALSE THEN [1] ELSE [] END | "
			"    MERGE (fav_user:USER {id:{favourited_by}}) "
			"    CREATE (fav_user)-[:LIKES {on:{fav_timestamp}}]->(tweet), "
			"      (frame_fav) -[:HAS_FAV]-> (fe:FAV_EVENT {timestamp:{fav_timestamp}}),"
			"      (fe) -[:FAV_USER]-> (fav_user),"
			"      (fe) -[:FAV_TWEET]-> (tweet) ) "
			") "
			"WITH tweet "
			# Proceed only if the tweet was not already created
			"MATCH (tweet) WHERE NOT (tweet) -[:INFO]-> () "
			# Create user and then the relationships
			"FOREACH (x IN CASE WHEN {user_in_lru} = TRUE THEN [1] ELSE [] END | "
			"  MERGE (user_lru:USER_LRU {id:{user_id}}) "
			"  CREATE (user_lru) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}), "
			"    (frame) -[:HAS_TWEET]-> (te:TWEET_EVENT {timestamp:{created_at}}),"
			"    (te) -[:TE_USER]-> (user_lru),"
			"    (te) -[:TE_TWEET]-> (tweet) ) "
			"FOREACH (x IN CASE WHEN {user_in_lru} = FALSE THEN [1] ELSE [] END | "
			"  MERGE (user:USER {id:{user_id}}) "
			"  CREATE (user) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}), "
			"    (frame) -[:HAS_TWEET]-> (te:TWEET_EVENT {timestamp:{created_at}}),"
			"    (te) -[:TE_USER]-> (user),"
			"    (te) -[:TE_TWEET]-> (tweet) ) "
			# Create links to hashtags, mentions, urls
			"FOREACH ( hashtag in {hashtags} | "
			"  MERGE (hashtag_node:HASHTAG {text:hashtag}) "
			"  CREATE (tweet) -[:HAS_HASHTAG {on:{created_at}}]-> (hashtag_node) ) "
			"FOREACH ( mention_id in {mention_ids_lru} |  "
			"  MERGE (mention_node:USER_LRU {id:mention_id}) "
			"  CREATE (tweet) -[:HAS_MENTION {on:{created_at}}]-> (mention_node) ) "
			"FOREACH ( mention_id in {mention_ids_non_lru} |  "
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
			"hashtags":hashtags, "mention_ids_lru":mention_ids_lru, "mention_ids_non_lru":mention_ids_non_lru, "urls":urls,
			"quoted_status":quoted_status, "quoted_status_id":quoted_status_id,
			"in_reply_to_status_id": in_reply_to_status_id, "frame_start_t":frame_start_t, "frame_end_t":frame_end_t,
			"favourited_by":favourited_by, "fav_timestamp":fav_timestamp, "fav_frame_start_t":fav_frame_start_t,
			"fav_frame_end_t":fav_frame_end_t, "fav_by_in_lru":fav_by_in_lru, "user_in_lru":user_in_lru, 
			"lru_evict_users":lru_evict_users, "lru_add_users":lru_add_users})
		# Can remove quoted_tweet field from tweet

################################################################
def clear_db():
	session.run("MATCH (n) DETACH DELETE n")
	for index in session.run("CALL db.indexes()"):
		session.run("DROP "+index["description"])

def create_indexes():
	session.run("CREATE INDEX ON :USER(id)")
	session.run("CREATE INDEX ON :USER_LRU(id)")
	session.run("CREATE INDEX ON :TWEET(id)")
	session.run("CREATE INDEX ON :HASHTAG(text)")
	session.run("CREATE INDEX ON :HASHTAG_LRU(text)")
	session.run("CREATE INDEX ON :URL(url)")
	session.run("CREATE INDEX ON :URL_LRU(url)")
	session.run("CREATE INDEX ON :FRAME(start_t)")
################################################################

def getDateFromTimestamp(timestamp):
	return datetime.fromtimestamp(timestamp).strftime('%a %b %d %H:%M:%S +0000 %Y')

def get_user_screen_names(filename):
	f = open(filename,'r')
	ret = []
	for name in f:
		ret.append(name.strip())
	return ret

def getScreenNameToUserIdMap(user_screen_names):
	ret = {}
	with open("data/timestamps.txt","r") as f:
		timestamps = [x.rstrip() for x in f.readlines()]
	for time_str in timestamps:
		# log("Starting for %s"%(time_str))
		timestamp = datetime.strptime(time_str,'%Y-%m-%d %H-%M-%S.%f').timestamp()
		for screen_name in user_screen_names:
			if(screen_name not in ret):
				user_info_file = 'data/user_info/'+screen_name+"_"+time_str+'.txt'
				try:
					with open(user_info_file, 'r') as f:
						# log("\t%s"%(screen_name))
						ret[screen_name] = json.loads(f.read())['id']
				except FileNotFoundError:
					pass
	return ret

def readDataAndCreateGraph(filename_screen_names, STAGE_BY_STAGE):
	user_screen_names = get_user_screen_names(filename_screen_names)
	screenNameToUserId = getScreenNameToUserIdMap(user_screen_names)
	log('Starting: %s'%(str(datetime.now().timestamp())))
	log("Size of map %s"%(str(len(screenNameToUserId))))
	with open("data/timestamps.txt","r") as f:
		timestamps = [x.rstrip() for x in f.readlines()]

	stages_left = 5
	if (not STAGE_BY_STAGE):
		stages_left = 1

	while (stages_left > 0):
		stages_left -= 1
		for time_str in timestamps:
			log("Starting for %s"%(time_str))
			timestamp = datetime.strptime(time_str,'%Y-%m-%d %H-%M-%S.%f').timestamp()
			for screen_name in user_screen_names:
				to_print = "\t" + screen_name + " :"
				user_info_file = 'data/user_info/'+screen_name+"_"+time_str+'.txt'
				tweet_file     = 'data/tweets/'+screen_name+"_"+time_str+".txt"
				favorite_file  = 'data/favourites/'+screen_name+"_"+time_str+'.txt'
				follower_file  = 'data/user_followers/'+screen_name+"_"+time_str+'.txt'
				friends_file   = 'data/user_friends/'+screen_name+"_"+time_str+'.txt'
				user_id = screenNameToUserId[screen_name]
				
				if ((STAGE_BY_STAGE and stages_left == 4) or (not STAGE_BY_STAGE)):
					try:
						with open(user_info_file, 'r') as f:
							user_info = json.loads(f.read())
							t = datetime.now().timestamp()
							update_user(user_id,user_info,timestamp)
							sync_session(); 
							to_print += " Profile %f"%(datetime.now().timestamp()-t)
					except FileNotFoundError:
						pass
				
				if ((STAGE_BY_STAGE and stages_left == 3) or (not STAGE_BY_STAGE)):
					try:
						with open(tweet_file, 'r') as f:
							count = 0
							tweets_list_list = json.loads(f.read())
							t = datetime.now().timestamp()
							for tweet_list in tweets_list_list:
								for tweet in tweet_list:
									create_tweet(tweet=tweet)
									count += 1
							to_print += " Tweets (%d) %f"%(count,datetime.now().timestamp()-t)
							to_print += sync_session('TWEET'); 
							user_lru.printState()
					except FileNotFoundError:
						pass

				if ((STAGE_BY_STAGE and stages_left == 2) or (not STAGE_BY_STAGE)):
					try:
						with open(favorite_file, 'r') as f:
							count = 0
							tweets_list_list = json.loads(f.read())
							t = datetime.now().timestamp()
							for tweet_list in tweets_list_list:
								for tweet in tweet_list:
									create_tweet(tweet=tweet, favourited_by=user_id, fav_timestamp=timestamp)
									count += 1
							to_print += " Favorites (%d) %f"%(count,datetime.now().timestamp()-t)
							to_print += sync_session('TWEET'); 
							user_lru.printState()
					except FileNotFoundError:
						pass

				if ((STAGE_BY_STAGE and stages_left == 1) or (not STAGE_BY_STAGE)):
					try:
						with open(follower_file, 'r') as f:
							followers = json.loads(f.read())
							t = datetime.now().timestamp()
							update_followers(user_id, followers, timestamp)
							sync_session(); 
							to_print += " Followers (%d) %f"%(len(followers),datetime.now().timestamp()-t)
					except FileNotFoundError:
						pass

				if ((STAGE_BY_STAGE and stages_left == 0) or (not STAGE_BY_STAGE)):
					try:
						with open(friends_file, 'r') as f:
							friends = json.loads(f.read())
							t = datetime.now().timestamp()
							update_friends(user_id, followers, timestamp)
							sync_session(); 
							to_print += " Friends (%d) %f"%(len(friends),datetime.now().timestamp()-t)
					except FileNotFoundError:
						pass
				
				if (to_print != ("\t" + screen_name + " :")):
					log(to_print)

def simulateExample():
	IS_REAL_DATA['value'] = False
	timestamp = 0
	print('Starting: %s'%(str(datetime.now().timestamp())))
	update_user(1,{"m1":"d1","m2":"d2"},timestamp); sync_session(); print("User: ", datetime.now().timestamp())
	update_user(1,{"m1":"d3","m2":"d4"},timestamp+1); sync_session(); print("User: ", datetime.now().timestamp())
	update_user(1,{"m1":"d5","m2":"d6"},timestamp+2); sync_session(); print("User: ", datetime.now().timestamp())
	update_followers(1, ["f1","f2"], timestamp+3); sync_session(); print("Followers: ", datetime.now().timestamp())
	update_followers(1, ["f2","f3"], timestamp+4); sync_session(); print("Followers: ", datetime.now().timestamp())
	update_followers(1, ["f1","f2"], timestamp+5); sync_session(); print("Followers: ", datetime.now().timestamp())
	update_followers(1, [],          timestamp+6); sync_session(); print("Followers: ", datetime.now().timestamp())
	update_followers(1, ["f1","f4"], timestamp+7); sync_session(); print("Followers: ", datetime.now().timestamp())
	update_friends(1, ["g1","g2"], timestamp+3); sync_session(); print("Friends: ", datetime.now().timestamp())
	update_friends(1, ["g2","g3"], timestamp+4); sync_session(); print("Friends: ", datetime.now().timestamp())
	update_friends(1, ["g1","g2"], timestamp+5); sync_session(); print("Friends: ", datetime.now().timestamp())
	update_friends(1, [],          timestamp+6); sync_session(); print("Friends: ", datetime.now().timestamp())
	update_friends(1, ["g1","g4"], timestamp+7); sync_session(); print("Friends: ", datetime.now().timestamp())

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

	create_tweet(tweet=tweet1, favourited_by=2, fav_timestamp=20); sync_session(); print("Tweet: ", datetime.now().timestamp())
	create_tweet(tweet2); sync_session(); print("Tweet: ", datetime.now().timestamp())
	create_tweet(tweet3, favourited_by=5, fav_timestamp=20); sync_session(); print("Tweet: ", datetime.now().timestamp())
	create_tweet(tweet4); sync_session(); print("Tweet: ", datetime.now().timestamp())
	create_tweet(tweet5); sync_session(); print("Tweet: ", datetime.now().timestamp())

################################################################

clear_db()
create_indexes()

start_time = datetime.now().timestamp()

# simulateExample()
readDataAndCreateGraph('users2_filtered.txt', STAGE_BY_STAGE=True)

session_close_start_t = datetime.now().timestamp()
session.close()
log("Closing session took: %f"%(datetime.now().timestamp()-session_close_start_t))

log("Closing: %s"%(str(datetime.now().timestamp())))

end_time = datetime.now().timestamp()
log("Time taken: %s"%(str(end_time-start_time)))


'''
QUERIES
=======
All events between t1 and t2
----------------------------
WITH 1 AS t1, 4 AS t2 
MATCH (run:RUN) -[:HAS_FRAME]-> (frame:FRAME)
WHERE frame.end_t >= t1 AND frame.start_t <= t2 
MATCH (frame) -[]-> (event) -[]-> (actor)
WHERE event.timestamp <= t2 AND event.timestamp >= t1
RETURN frame,event,actor
'''