"""
Module to read tweets from files and post them to a Kafka topic (currently "tweets_topic") for any downstream processes.

.. note::
	Currently, only the Flink applications are reading tweets from this Kafka topic.
	In our proposed architecture, MongoDB and Neo4j ingestion applications should also read tweets from this Kafka topic.
	However this is not integrated yet, hence MongoDB and Neo4j applications currenlty read tweets from files themselves.
	This should be integrated as such.
"""
from kafka import KafkaProducer
import json, datetime, time, os, json, threading, signal
from datetime import datetime
from threading import Thread
from queue import Queue, Empty

# *************************************************** 
class ServiceExit(Exception):
	""" Custom exception which is used to trigger the clean exit
	of all running threads and the main program. """
	pass

def service_shutdown(signum, frame):
	print('Caught signal %d' % signum)
	raise ServiceExit

# *************************************************** 
class Producer:
	"""
	Class that on initialization, spawns 2 threads. One thread keeps reading tweets from the files and puts them to a queue.
	Second thread keeps reading tweets from the queue and puts them to the Kafka topic.
	"""

	def __init__(self, tweet_folder_name, max_q_size):
		
		# This thread function will keep reading tweets from file and put them into queue
		def read_and_put_tweets(q, folder_name):
			print('Starting thread - read_and_put_tweets')
			currentThread = threading.currentThread()
			files = [x for x in os.listdir(folder_name)]
			files.sort() # to sort by date and time; expects filenames to be such that sorting on name will sort on time as well
			for filename in files:
				# print(filename)
				print(filename)
				filepath = os.path.join(folder_name, filename)
				with open(filepath, 'r') as f:
					print("Reading file")
					tweets = json.loads(f.read())
					print("Reading file completed")
					for i,tweet in enumerate(tweets):
						# check if the kill signal has been received
						if not getattr(currentThread, "do_run", True):
							print("Stopping thread - read_and_put_tweets")
							return
						# tweets that are of the type "delete" or "status_withheld" are ignored
						if ("delete" in tweet or "status_withheld" in tweet):
							continue
						while(True):
							try:
								# check if the kill signal has been received
								if not getattr(currentThread, "do_run", True):
									print("Stopping thread - read_and_put_tweets")
									return
								q.put(tweet, block=True, timeout=10)
								break
							except:
								print('Queue seems to be full. Size: %d.'%(q.qsize()))
						if ((i+1)%10000 == 0):
							print("Populated %d tweets from this file."%i)
					print("Completed this file.")
		
		# This thread function will keep putting tweets from queue to kafka
		def put_tweets_to_kafka(q, kafkaProducer, kafkaTopic):
			print('Starting thread - put_tweets_to_kafka')
			currentThread = threading.currentThread()
			is_first = True
			# to simulate the actual time gap between the tweets, we find the difference between current time 
			# and first tweet's time and shift each tweet's virtual creation time that many seconds later
			time_to_shift_in_ms = None
			while(getattr(currentThread, "do_run", True)):
				try:
					tweet = q.get(block=True, timeout=10)
					if is_first:
						time_to_shift_in_ms = int(time.time()*1000)-int(tweet["timestamp_ms"])
						is_first = False
					modified_timestamp = int(tweet["timestamp_ms"])+time_to_shift_in_ms
					# wait until the tweet's virtual creation time hasn't been reached
					if modified_timestamp > int(time.time()*1000):
						time.sleep((modified_timestamp-int(time.time()*1000))/1000)
					kafkaProducer.send(kafkaTopic, tweet)
				except Empty:
					print('Queue seems to be empty. Size: %d.'%(q.qsize()))
				except Exception as e:
					print('Exception while putting tweet to kafka. %s:%s'%(str(type(e)),str(e)))
			print("Stopping thread - put_tweets_to_kafka")
		
		self._q = Queue(maxsize=max_q_size)
		self.kafkaProducer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
		self.kafkaTopic = 'tweets_topic'

		# Start the job threads
		try:
			self.t1 = Thread(target = read_and_put_tweets, 
				args = (self._q, tweet_folder_name))
			self.t2 = Thread(target = put_tweets_to_kafka, 
				args = (self._q, self.kafkaProducer, self.kafkaTopic))
			self.t1.daemon = False
			self.t2.daemon = False
			self.t1.start()
			self.t2.start()	 
			# Keep the main thread running, otherwise signals are ignored.
			while True:
				time.sleep(0.5)
	 
		except ServiceExit:
			# Terminate the running threads.
			# Set the shutdown flag on each thread to trigger a clean shutdown of each thread.
			self.t1.do_run = False
			self.t2.do_run = False
			# Wait for the threads to close...
			self.t1.join()
			self.t2.join()
	 
		print('Exiting main program')



# *************************************************** 

if __name__=='__main__':

	signal.signal(signal.SIGTERM, service_shutdown)
	signal.signal(signal.SIGINT, service_shutdown)
	producer = Producer(tweet_folder_name='../data_collection/data', max_q_size=100000)


# THE FOLLOWING WAS USED INITITALLY FOR TESTING THE FLINK APPLICATION BY SENDING CUSTOM TWEET OBJECTS

# producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
# topic = 'tweets_topic'

# id = 1
# for i in range(1,6,1):
# 	d = {'id_str': id,
# 	'user':{'id_str':'1'},
# 	'created_at':datetime.fromtimestamp(i).strftime("%a %b %d %H:%M:%S %z %Y"), 
# 	'entities':{'hashtags':[{'text':'1'}, {'text':'2'}],
# 				'urls':[{'expanded_url':'u'}]},
# 	'timestamp_ms':str(i*1000)
# 	}
# 	producer.send(topic, d)
# 	id += 1
# 	d = {'id_str': id,
# 	'user':{'id_str':'2'},
# 	'created_at':datetime.fromtimestamp(i).strftime("%a %b %d %H:%M:%S %z %Y"), 
# 	'entities':{'hashtags':[{'text':'1'}],
# 				'urls':[{'expanded_url':'u'}]},
# 	'timestamp_ms':str(i*1000)
# 	}
# 	producer.send(topic, d)
# 	id += 1

# for i in range(6, 11, 1):
# 	d = {'id_str': id,
# 	'user':{'id_str':'1'},
# 	'created_at':datetime.fromtimestamp(i).strftime("%a %b %d %H:%M:%S %z %Y"), 
# 	'entities':{'hashtags':[{'text':'1'}],
# 				'urls':[{'expanded_url':'u'}]},
# 	'timestamp_ms':str(i*1000)
# 	}
# 	producer.send(topic, d)
# 	id += 1
# 	d = {'id_str': id,
# 	'user':{'id_str':'1'},
# 	'created_at':datetime.fromtimestamp(i).strftime("%a %b %d %H:%M:%S %z %Y"), 
# 	'entities':{'hashtags':[{'text':'1'}],
# 				'urls':[{'expanded_url':'u'}]},
# 	'timestamp_ms':str(i*1000)
# 	}
# 	producer.send(topic, d)
# 	id += 1

# producer.flush()