"""
Module to execute mongoDB queries. The idea is to keep the  mongo interface minimal and easily extensible, and thus
only pre specified queries can be answered through mongoDB, rather than generic ones.

The :mod:`execute_queries` module contains the classes:

- :class:`execute_queries.MongoQuery`

One can use the different function in the class to execute different queries

Example illustrating how to answer different queries.

>>> q = MongoQuery()
>>> print(q.mp_ht_in_total(limit=10)) # get 10 most popular hashtags
>>> print(q.mp_um_in_total(10)) # get 10 most popular users
>>> print(q.mp_ht_in_interval(10, 1500486521,1501496521)) # get 10 most popular hashtags in interval
>>> print(q.ht_in_interval("baystars",1500486521,1501496521)) # get the timestamps at which baystars is used in interval
>>> print(q.ht_with_sentiment("baystars",1500486521,1501496521)) # get the timestamps and sentiment at which baystars is used in interval

"""
from __future__ import print_function
import pymongo
from pymongo import MongoClient
from pprint import *
from datetime import datetime
from collections import defaultdict
import time,os,json
from bson.son import SON


class MongoQuery():
	"""
	Class to answer mongoDB queries. Make connection to the database and keep on answering queris untill the
	object is deleted
	"""
	def __init__(self):
		self.client = MongoClient('mongodb://localhost:27017/')
		self.db = self.client['regular_interval']
		# self.hashtag_collection = self.db['hashtags']

	def clear_db(self):
		"""
		Delete all the collections
		"""
		print("Clearing out the complete mongoDB....")
		self.db.ht_collection.remove({})
		self.db.url_collection.remove({})
		self.db.um_collection.remove({})
		print("mongoDB deleted")

	def mp_ht_in_total(self,limit):
		"""
		Give <limit> most popular hashtags in total

		:param limit: number of records to return
		"""
		limit = int(limit)
		pipeline = [{"$group": {"_id": "$hashtag", "count": {"$sum": 1}}},{"$sort": {"count":-1}},{"$limit":limit}]
		l = self.db.ht_collection.aggregate(pipeline)["result"]
		return {"hashtag":[x["_id"] for x in l],"count":[x["count"] for x in l]}

	def mp_ht_in_interval(self,limit,begin,end):
		"""
		Function to give the most popular hashtags in the time interval <begin> and <end>

		:param limit: number of records to return
		:param begin: the begining unix time timestamp of the interval
		:param end: the ending unix time timestamp of the interval
		"""
		limit = int(limit)
		t1 = int(begin)
		t2 = int(end)
		# t1,t2 = begin,end
		pipeline = [{"$match":{"timestamp":{"$gte":t1,"$lte":t2}}},{"$group": {"_id": "$hashtag", "count": {"$sum": 1}}},
		{"$sort": {"count":-1}},{"$limit":limit}]
		l =  self.db.ht_collection.aggregate(pipeline)["result"]
		return {"hashtag":[x["_id"] for x in l],"count":[x["count"] for x in l]}

	def ht_in_interval(self,hashtag,begin,end):
		"""
		Give the timetamps at which <hashtag> is used between <begin> and <end>

		:param hashtag: hashtag for the query
		:param begin: the begining unix time timestamp of the interval
		:param end: the ending unix time timestamp of the interval
		"""
		# t1 = begin.timestamp()
		# t2 = end.timestamp()
		t1,t2 = int(begin),int(end)
		records = self.db.ht_collection.find({"hashtag":hashtag,"timestamp":{"$gte":t1,"$lte":t2}},{"timestamp":1})
		l = [x["timestamp"] for x in list(records)]
		return {"timestamps":l}

	def ht_with_sentiment(self,hashtag,begin,end):
		"""
		Give the timetamps at which <hashtag> is used and and sentiment of tweet in which <hashtag> occured between <begin> and <end>

		:param hashtag: hashtag for the query
		:param begin: the begining unix time timestamp of the interval
		:param end: the ending unix time timestamp of the interval
		"""
		# t1 = begin.timestamp()
		# t2 = end.timestamp()
		t1,t2 = int(begin),int(end)
		records = self.db.ht_collection.find({"hashtag":hashtag,"timestamp":{"$gte":t1,"$lte":t2}},{"timestamp":1,"sentiment_pos":1,"sentiment_neg":1})
		l = [(x["timestamp"],x["sentiment_pos"],x["sentiment_neg"]) for x in list(records)]
		return {"timestamps":[x[0] for x in l],"positive_sentiment":[x[1] for x in l],"negative_sentiment":[x[2] for x in l]}

	def mp_um_in_total(self,limit):
		"""
		Give <limit> most popular users(in iterms of mentions) in total

		:param limit: number of records to return
		"""
		limit = int(limit)
		pipeline = [{"$group": {"_id": "$user", "count": {"$sum": 1}}},{"$sort": {"count":-1}},{"$limit":limit}]
		l = self.db.um_collection.aggregate(pipeline)["result"]
		return {"userId":[int(x["_id"]) for x in l],"count":[x["count"] for x in l]}

if __name__=="__main__":
	q = MongoQuery()
	print(q.mp_ht_in_total(limit=10))
	print(q.mp_um_in_total(10))
	print(q.mp_ht_in_interval(10,1500486521,1501496521))
	print(q.ht_in_interval("baystars",1500486521,1501496521))
	print(q.ht_with_sentiment("baystars",1500486521,1501496521))
