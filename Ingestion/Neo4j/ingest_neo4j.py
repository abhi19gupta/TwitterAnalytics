"""
Module to insert data into neo4j database.

The :mod:`ingest_neo4j` module contains the classes:

- :class:`ingest_neo4j.Twitter`

One can use the :func:`ingest_neo4j.Twitter.ingest_tweet` to insert a new tweet into the database.

An example usage where we want to insert all tweets from all files in a folder tweet_folder:

>>> t = Twitter(50000)
>>> t.get_constraints()
>>> t.get_profile()
>>> read_tweets(<tweet_folder>, t)
>>> t.get_profile()
>>> t.close_session()
"""
from __future__ import print_function
from py2neo import Graph, Node, Relationship
from datetime import datetime
import json, time, os
from pprint import pprint
from neo4j.v1 import GraphDatabase, basic_auth
import logging, copy
from multiprocessing import Process, Event, Queue
import multiprocessing

count = 0
log_file = "neo4j_logs.txt"

FRAME_DELTA_T = 60*60
def getDateFromTimestamp(timestamp):
	return datetime.fromtimestamp(timestamp).strftime('%a %b %d %H:%M:%S +0000 %Y')
def getFrameStartEndTime(timestamp):
	start = FRAME_DELTA_T*(timestamp//FRAME_DELTA_T)
	end = start + FRAME_DELTA_T - 1
	return (start,end)
def flatten_json(json_obj):
	"""
	Function to flatten the tweet. Used in case we want to store the complete tweet JSON in the TWEET node.
	This is because neo4j doesn't allow nested jsons to be stored
	"""
	json_fields = []
	for key in json_obj:
		if type(json_obj[key]) is dict:
			json_obj[key] = json.dumps(json_obj[key])
			json_fields.append(key)
	json_obj["json_fields"] = json_fields # while fetching convert these fields back to jsons

def log(text):
	f = open(log_file,'a')
	f.write(text)
	f.write('\n')
	f.close()

class Twitter:
	"""
    Class containing functions to insert tweets into neo4j.
    We open a connection to the neo4j database through py2neo and the official
    neo4j driver. Dealing with transactions is easy in py2neo, so it is used to make and commit transactions. While
    the connection to the neo4j driver is used just in clearing out the graph.

    :param batch_size: number of tweets to take into transaction before commiting it
    """
	def __init__(self,batch_size=200,):

		self.batch_size = batch_size
		self.tweet_counter = 0
		self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
		self.session = self.driver.session()
		self.graph = Graph("bolt://localhost:7687",password="password")
		self.tweet_tx = self.graph.begin()
		self.time = time.time()


	def clear_graph(self):
		"""
		Delete the complete graph. Albiet, keep the indices.
		"""
		print("Clearing out the complete graph....")
		self.session.run("MATCH (n) DETACH DELETE n")
		# This is to delete the graph using py2neo. Found to be too slow. Thus make use of the neo4j driver.
		# self.graph.delete_all()
		# for index in session.run("CALL db.indexes()"):
		# 	session.run("DROP "+index["description"])
		print("Graph deleted")

	def get_constraints(self):
		"""
		Get the constrainsts on different types of nodes.
		"""
		print(self.graph.schema.get_uniqueness_constraints("FRAME"))
		# print(self.graph.schema.get_uniqueness_constraints("TWEET_EVENT"))
		print(self.graph.schema.get_uniqueness_constraints("USER"))
		print(self.graph.schema.get_uniqueness_constraints("TWEET"))
		print(self.graph.schema.get_uniqueness_constraints("HASHTAG"))
		print(self.graph.schema.get_uniqueness_constraints("URL"))

	def drop_constraint(self,node,attrib):
		"""
		Drop constraint on attrib in node node

		:param node: node type on which to delete the constraint
		:parem attrib: node attibute whose constraint is to be deleted
		"""
		self.graph.schema.drop_uniqueness_constraint(node,attrib)

	def create_constraints(self):
		"""
		Create uniqueness constraints on the attributes of nodes. Note that ceating a constraint automatically
		creates an index on it
		"""
		self.create_constraint("FRAME","start_t")
		self.create_constraint("TWEET","id")
		self.create_constraint("USER","id")
		self.create_constraint("HASHTAG","text")
		self.create_constraint("URL","url")

	def create_constraint(self,node,attrib):
		"""
		Create constraint on attrib in node node

		:param node: node type on which to create the constraint
		:parem attrib: node attibute whose constraint is to be created
		"""
		self.graph.schema.create_uniqueness_constraint(node,attrib)

	def get_profile(self):
		"""
		The number of total, user, tweet, hashtag nodes in the graph
		"""
		total = list(self.session.run("MATCH(n) RETURN COUNT(n)"))[0]
		tweets = list(self.session.run("MATCH(n:TWEET) RETURN COUNT(n)"))[0]
		users = list(self.session.run("MATCH(n:USER) RETURN COUNT(n)"))[0]
		hashtags = list(self.session.run("MATCH(n:HASHTAG) RETURN COUNT(n)"))[0]
		print("Count of total = ",total,"tweets = ",tweets,"users=",users)

	def close(self):
		"""
		See if there are some tweets not comitted in the trnx and if yes, commit those.
		"""
		if(not(self.tweet_tx.finished())):
			print("cleaning up")
			self.tweet_tx.commit()
			self.tweet_tx = self.graph.begin()

	def close_session(self):
		"""
		Close the neo4j driver session
		"""
		self.session.close()

	# @profile
	def insert_tweet(self,tweet, favourited_by=None, fav_timestamp=None):
		"""
		The main function to insert the tweet. Begin a transaction, when atleast batch_size number of tweets
		are collected, commit the transaction. Start collecting the new tweets after that.

		We have two cases depending if the tweet to be inserted is a
		retweet or not. We mention the steps taken to insert the tweet in the two case:

		* The tweet is a retweet
			- Create a tweet_event under a appropriate frame
			- Merge node for this tweet. Maybe the tweet node already partially exists because some other tweet is its reply.
			- Create favorite relation if needed
			- Proceed only if the tweet was not already created
			- Create user and then the relationships
			- Find node of original tweet and link

		* The tweet is not a retweet
			- Create a tweet_event under a appropriate frame
			- Merge node for this tweet
			- Create favorite relation if needed
			- Proceed only if the tweet was not already created
			- Create user and then the relationships
			- Create links to hashtags, mentions, urls
			- Create link to quoted tweet in case this tweet quotes another tweet
			- Create link to original tweet in case this is a reply tweet

		:param tweet: the json of the tweet to be inserted
		:param favourited_by: userid of the user who favourited the tweet
		:param fav_timestamp: time at which the tweet was favourited
		:returns: None

		.. todo:: Currently we are making use of transactions only. We get decent peak ingestion rate of
			around 1000 tweets/sec. But this can be increased by overlapping the collection and ingestion part as
			we do in case of mongoDB. But the same scheme can't be used here as the transaction created is not
			a native python object and hance can't be passed between python multiprocessing module processes. So, one
			idea is to create a csv with the tweets, and then call the use_csv function in neo4j to ingest. But this is a
			contrived way of doing this by doing same task twice.

		"""
		global count

		if "user" not in tweet:
			print("user not found in tweet:")
			pprint(tweet)
			return

		self.tweet_counter+=1
		# print(count)
		user_id = tweet["user"]["id"]
		user_name = tweet["user"]["name"]
		user_screenname = tweet["user"]["screen_name"]
		tweet['created_at'] = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y').timestamp()

		(frame_start_t, frame_end_t) = getFrameStartEndTime(tweet['created_at'])
		(fav_frame_start_t, fav_frame_end_t) = (None,None) if fav_timestamp is None else getFrameStartEndTime(fav_timestamp)

		retweeted_status      = tweet.get("retweeted_status",None)
		quoted_status         = tweet.get("quoted_status",None)
		in_reply_to_status_id = tweet.get("in_reply_to_status_id",None)

		if retweeted_status is not None:
			self.insert_tweet(retweeted_status)
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
				"WITH frame, tweet "
				# Proceed only if the tweet was not already created
				"MATCH (tweet) WHERE NOT (tweet) -[:INFO]-> () "
				# Create user and then the relationships
				"MERGE (user:USER {id:{user_id}}) "
				"  SET user.name = {user_name}, user.screen_name = {user_screenname} "
				"CREATE (user) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}), "
				"  (frame) -[:HAS_TWEET]-> (te:TWEET_EVENT {timestamp:{created_at}}),"
				"  (te) -[:TE_USER]-> (user),"
				"  (te) -[:TE_TWEET]-> (tweet) "
				# Find node of original tweet and link
				"WITH tweet "
				"MATCH (original_tweet:TWEET {id:{original_tweet_id}}) "
				"CREATE (tweet) -[:RETWEET_OF {on:{created_at}}]-> (original_tweet) ",
				{"user_id":user_id, "user_name":user_name, "user_screenname":user_screenname, "tweet_id":tweet["id"],
				"created_at":tweet["created_at"] ,"tweet":{},
				"original_tweet_id":retweeted_status["id"], "frame_start_t":frame_start_t, "frame_end_t":frame_end_t,
				"favourited_by":favourited_by, "fav_timestamp":fav_timestamp, "fav_frame_start_t":fav_frame_start_t,
				"fav_frame_end_t":fav_frame_end_t})
				# Can remove the retweeted_status field from tweet
		else:
			hashtags    = [x["text"] for x in tweet["entities"]["hashtags"]]
			mention_ids = [x["id"]   for x in tweet["entities"]["user_mentions"]]
			mention_names = [x["name"]   for x in tweet["entities"]["user_mentions"]]
			mention_screennames = [x["screen_name"]   for x in tweet["entities"]["user_mentions"]]
			urls        = [{"url": x["url"], "expanded_url": x["expanded_url"]} for x in tweet["entities"]["urls"]]

			if quoted_status is not None:
				self.insert_tweet(tweet["quoted_status"])

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
				"WITH frame, tweet "
				# Proceed only if the tweet was not already created
				"MATCH (tweet) WHERE NOT (tweet) -[:INFO]-> () "
				# Create user and then the relationships
				"MERGE (user:USER {id:{user_id}}) "
				"  SET user.name = {user_name}, user.screen_name = {user_screenname} "
				"CREATE (user) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}), "
				"  (frame) -[:HAS_TWEET]-> (te:TWEET_EVENT {timestamp:{created_at}}), "
				"  (te) -[:TE_USER]-> (user),"
				"  (te) -[:TE_TWEET]-> (tweet) "
				# Create links to hashtags, mentions, urls
				"FOREACH ( hashtag in {hashtags} | "
				"  MERGE (hashtag_node:HASHTAG {text:hashtag}) "
				"  CREATE (tweet) -[:HAS_HASHTAG {on:{created_at}}]-> (hashtag_node) ) "
				"FOREACH ( i in RANGE(0,size({mention_ids})-1) |  "
				"  MERGE (mention_node:USER {id:{mention_ids}[i]}) "
				"    SET mention_node.name = {mention_names}[i], mention_node.screen_name = {mention_screennames}[i] "
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
				{"user_id":user_id, "user_name":user_name, "user_screenname":user_screenname, "tweet_id":tweet["id"],
				"created_at":tweet["created_at"] ,"tweet":{},
				"hashtags":hashtags, "mention_ids":mention_ids, "mention_names":mention_names, "mention_screennames":mention_screennames,
				"urls":urls, "quoted_status":quoted_status, "quoted_status_id":quoted_status_id,
				"in_reply_to_status_id": in_reply_to_status_id, "frame_start_t":frame_start_t, "frame_end_t":frame_end_t,
				"favourited_by":favourited_by, "fav_timestamp":fav_timestamp, "fav_frame_start_t":fav_frame_start_t,
				"fav_frame_end_t":fav_frame_end_t})
		# print(self.tweet_counter)
		if(self.tweet_counter>=self.batch_size):
			log("commiting the tweet_transaction %s,%.2f"%(str((time.time()-self.time)/self.batch_size),1.0/((time.time()-self.time)/self.batch_size)))
			self.time = time.time()
			self.tweet_tx.commit()
			self.tweet_counter = 0
			self.tweet_tx = self.graph.begin()

def read_tweets(path, twitter, filename=""):
	"""
	Read tweets from the directory in path and inert all tweets in all files in the first level of path into
	neo4j.

	:param path: the path of the directory
	:param twitter: a Twitter object
	:param filename: optional, if want to insert tweets from a single file
	"""
	global count
	files = [x for x in os.listdir(path)]
	files.sort()
	print("Starting to ingest tweets in Neo4j")
	for file in files:
		print("Reading file: %s"%file)
		# if (file!=filename):
		# 	continue
		fin = open(path+"/"+file)
		# s = fin.read().replace("null","'null'").replace("false","False").replace("true","True")
		l = json.loads(fin.read())
		print("Number of tweets ",len(l))
		for i,twt in enumerate(l):
			if (i%10000 == 0):
				print(i, count)
			if ("delete" in twt or "status_withheld" in twt):
				continue
			try:
				twitter.insert_tweet(twt)
			except Exception as e:
				log("Failed to insert tweet: %s, %s, %s"%(type(e),e,twt))
			count+=1
		l.clear()
		print("Completed the file. Number of tweets till now: %d"%count)
	twitter.close()
	print("Ingestion process is done")


if __name__=="__main__":
	t = Twitter(50000)
	# t.clear_graph()
	# t.create_constraints()
	t.get_constraints()
	t.get_profile()
	# read_tweets("/home/db1/Desktop/AbhishekBackup/TwitterAnalytics/data/tweets")
	# read_tweets("/home/db1/Documents/data_collection/data_splitted", t)
	t.get_profile()
	t.close_session()
