from __future__ import print_function
import threading
import pymongo
from pymongo import MongoClient
from pprint import *
from datetime import datetime
from collections import defaultdict
from copy import *
import time,os,json
import logging
from multiprocessing import Process, Event, Queue
import multiprocessing
import numba as nb
import numpy as np
from bson.son import SON

logging.basicConfig(filename="debug_logs.txt",level=logging.DEBUG)
count = 0

def getDateFromTimestamp(timestamp):
	return datetime.fromtimestamp(timestamp).strftime('%a %b %d %H:%M:%S +0000 %Y')

def threaded(fn):
	def wrapper(*args, **kwargs):
		thread = threading.Timer(10,fn, args=args, kwargs=kwargs)
		thread.start()
		return thread
	return wrapper
class Timer(Process):
	"""Calls a function after a specified number of seconds:

	>>> t = Timer(30.0, f, args=None, kwargs=None)
	>>> t.start()
	>>> t.cancel() #stops the timer if it is still waiting

	"""
	def __init__(self, interval, function, args=None, kwargs=None, iterations=1, infinite=False):
		Process.__init__(self)
		self.interval = interval
		self.function = function
		self.args = args if args is not None else []
		self.kwargs = kwargs if kwargs is not None else {}
		self.finished = Event()
		self.infinite = infinite

		if infinite:
			self.iterations = infinite
			self.current_iteration = infinite
		else:
			self.iterations = iterations
			self.current_iteration = 1

	def cancel(self):
		"""Stop the timer if it hasn't already finished."""
		self.finished.set()

	def run(self):
		while not self.finished.is_set() and self.current_iteration <= self.iterations:
			self.finished.wait(self.interval)
			if not self.finished.is_set():
				self.function(*self.args, **self.kwargs)
			if not self.infinite:
				self.current_iteration += 1
		self.finished.set()

# @nb.jit(nb.types.Tuple((nb.int64, nb.int64))(nb.int64[:],nb.int64[:],nb.int64[:]),nopython=True,cache=True)
def calculate_sentiment(positive_words,negative_words,tweet_text):
		pos = 0
		neg = 0
		for x in tweet_text:
			if np.any(positive_words==x):
				pos+=1
			elif np.any(negative_words==x):
				neg+=1
		return(pos,neg)

class Ingest():
	def __init__(self, interval):
		self.interval = interval
		self.tweets = []

		self.positive_words = []
		self.negative_words = []
		with open("positive-words.txt","r",encoding = "ISO-8859-1") as fin:
			for line in fin:
				self.positive_words.append(hash(line.strip().lower()))
		with open("negative-words.txt","r",encoding = "ISO-8859-1") as fin:
			for line in fin:
				self.negative_words.append(hash(line.strip().lower()))
		self.positive_words = np.array(self.positive_words,dtype=np.int64)
		self.negative_words = np.array(self.negative_words,dtype=np.int64)

		self.current = int(time.time())
		self.lock = threading.Lock()


		self.q = Queue()
		self.proc = Process(target = self.worker,args=(self.q,))
		# self.proc1 = Process(target = self.worker,args=(self.q,))
		# self.proc.daemon = True
		self.proc.start()

	def exit(self):
		self.proc.join()

	def worker(self,q):
		#open connection to mongoDB
		client = MongoClient('mongodb://localhost:27017/')
		db = client['regular_interval']

		db.ht_collection.create_index("hashtag")
		db.url_collection.create_index("url")
		db.um_collection.create_index("user")

		db.ht_collection.create_index([("timestamp",pymongo.ASCENDING)])
		db.url_collection.create_index([("timestamp",pymongo.ASCENDING)])
		db.um_collection.create_index([("timestamp",pymongo.ASCENDING)])

		while(True):
			ts,t1 = q.get()
			print("prining in ",multiprocessing.current_process().name)
			pprint(len(t1))
			ht_l = []
			url_l = []
			um_l = []
			for twt in t1:
				for ht in twt["hashtags"]:
					ht_l.append({"timestamp":twt["timestamp"],"hashtag":ht,"sentiment_pos":twt["sentiment_pos"],"sentiment_neg":twt["sentiment_neg"]})
				for u in twt["urls"]:
					url_l.append({"timestamp":twt["timestamp"],"url":u,"sentiment_pos":twt["sentiment_pos"],"sentiment_neg":twt["sentiment_neg"]})
				for um in twt["user_mentions"]:
					um_l.append({"timestamp":twt["timestamp"],"user":um,"sentiment_pos":twt["sentiment_pos"],"sentiment_neg":twt["sentiment_neg"]})
			if(len(ht_l)>0):
				db.ht_collection.insert(ht_l)
			if(len(url_l)>0):
				db.url_collection.insert(url_l)
			if(len(um_l)>0):
				db.um_collection.insert(um_l)

	def populate(self):
		"""
		write to the mongoDB
		"""
		#some issue of thread safety here.
		global count
		# self.current = int(time.time())
		# print("Came here",self.current,count)
		# logging.debug("Time before copying =  %d ",int(time.time()))
		# with self.lock:
		print("length of tweets ",len(self.tweets))
		temp = self.tweets[:]
		self.tweets = []
		logging.debug("At time %d count = %d ",int(time.time()),count)

		self.q.put([self.current-self.interval,temp])
		print("putting into the q ",len(temp))
		self.current = self.current+self.interval

		thread = threading.Timer(self.interval, self.populate,[],{})
		# thread.daemon = True # We daemonize the thread, meaning when th main thread exits, this thread also exit safely
		thread.start()

	def aggregate(self):
		self.q1.put("signal")
		thread1 = threading.Timer(self.interval1, self.aggregate,[],{})
		thread1.start()

	def insert_tweet(self,tweet):
		"""
		update the in memory dictionaries
		"""
		l = np.array([hash(x.lower()) for x in tweet["text"].split() if (x[0]!="#" and x[0]!="@")],dtype=np.int64)
		pos,neg = calculate_sentiment(self.positive_words,self.negative_words,l)

		time_format = "%a %b %d %H:%M:%S +0000 %Y"
		d = datetime.strptime(tweet["created_at"],time_format)
		posix = time.mktime(d.timetuple())
		self.tweets.append({"timestamp":posix,"hashtags":[str.encode(x["text"]).decode('utf8','replace') for x in tweet["entities"]["hashtags"]],
			"urls":[str.encode(x["url"]).decode('utf8','replace') for x in tweet["entities"]["urls"]],
			"user_mentions":[x["id_str"] for x in tweet["entities"]["user_mentions"]],
			"sentiment_pos":pos,"sentiment_neg":neg})

def read_tweets(path,filename):
	global count
	ingest= Ingest(10)
	ll = []
	# fout = open("ll.txt","w")
	for file in os.listdir(path):
		if (file!=filename):
			continue
		# print(len(ll))
		fin = open(path+"/"+file)
		# s = fin.read().replace("null","'null'").replace("false","False").replace("true","True")
		l = json.loads(fin.read())
		# ll += [twt for sl in l for twt in sl]
		# if(len(ll)>500000):
		#     break
	print("We have total tweets ",len(l))
	print("Starting to simulate the process")
	# print(len(ll))
	ingest.populate()
	for i,twt in enumerate(l):
		if (i%10000 == 0):
			print(i)
		if ("delete" in twt or "status_withheld" in twt):
			continue
		ingest.insert_tweet(twt)
		count+=1
	ll = []
	print(count)
	print("Ingestion process is done")

if __name__=="__main__":
	##to simulate the process of periodic commit
	# i= Ingest(10)
	# i.populate()
	# print("------------")
	# time.sleep(2)
	# i.insert_tweet(tweet1)
	# time.sleep(10)
	# i.insert_tweet(tweet2)
	# i.insert_tweet(tweet3)
	# print("------------")
	# time.sleep(12)
	# i.insert_tweet(tweet1)

	q = MongoQuery()
	# q.clear_db()
	t1 = time.time()
	# read_tweets("/home/db1/Desktop/AbhishekBackup/TwitterAnalytics/data/tweets")
	# read_tweets("/home/db1/Documents/data_collection/data", "stream_out_2018-05-08 20-55-45.867977.txt")
	print("Done in time ",time.time()-t1)
	print(q.mp_ht_in_total(limit=10))
	print(q.mp_um_in_total())

	# print(q.mp_ht_in_interval(1500486521,1501496521))
	# print(q.ht_in_interval(1500486521,1501496521,"baystars"))
	# print(q.ht_with_sentiment(1500486521,1501496521,"baystars"))
