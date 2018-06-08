try:
	import json
except ImportError:
	import simplejson as json
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
import signal, sys
from datetime import datetime
import time
import ssl
ssl.match_hostname = lambda cert, hostname: True

class Logger:

	def __init__(self, total_tweets, batch_size, file_tweet_count):
		self.total_tweets = total_tweets
		self.batch_size = batch_size
		self.file_tweet_count = file_tweet_count
		self.unwritten_tweets = []
		self.configure_new_file()

	def configure_new_file(self):
		now = str(datetime.now())
		self.tweet_file_name = ('stream_out_%s.txt'%(now)).replace(':','-')
		self.log_file_name = ('log_%s.txt'%(now)).replace(':','-')
		self.current_file_count = 0
		with open(self.tweet_file_name, 'a') as f:
			f.write('[\n')

	def finish_current_file(self):
		with open(self.tweet_file_name, 'a') as f:
			f.write('\n]')

	def log(self, s):
		to_log = str(datetime.now()) + " | " + s + '\n'
		print(to_log)
		with open(self.log_file_name, 'a') as f:
			f.write(to_log)

	def write_tweet(self, tweet):
		self.unwritten_tweets.append(tweet)
		if len(self.unwritten_tweets) < self.batch_size:
			return
		# create new file if exceeds configuration
		if len(self.unwritten_tweets)+self.current_file_count > self.file_tweet_count:
			self.finish_current_file()
			self.configure_new_file()
		with open(self.tweet_file_name, 'a') as f:
			for tweet in self.unwritten_tweets:
				if self.current_file_count == 0:
					f.write(tweet)
				else:
					f.write(',\n'+tweet)
				self.current_file_count += 1
			self.unwritten_tweets = []


# ------------------------------------------------------ #

tweet_count = 1000000000 # total number of tweets to read
batch_size = 10000 # how many tweets to write together in 1 go
file_tweet_count = 500000 # max no of tweets to write in 1 file
logger = Logger(tweet_count, batch_size, file_tweet_count)

ACCESS_TOKEN = '894945534410080256-ty8NTmEAUzzwJQTjSAfbmGp81HSVcZb'
ACCESS_SECRET = 'plMGYeenmCZNs7gIDWrO17vEYFrm6GzgZ7BaJdPbMQYuL'
CONSUMER_KEY = 'QC5nVHYoVYdNbl0oQeGExCmWW'
CONSUMER_SECRET = 'e5EUAXqpYSjBZbhEpnqVCMd66WlSJTUJSCUdtQ5dIBmVlWYTIL'
oauth = OAuth(ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET)

def signal_handler(signal, frame):
		logger.log('You pressed Ctrl+C! Number of tweets read = %s'%str(num_read))
		logger.finish_current_file()
		sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
# ------------------------------------------------------ #

num_read = 0
while(True):
	twitter_stream = TwitterStream(auth=oauth)
	iterator = twitter_stream.statuses.sample()
	for tweet in iterator:
		# Twitter Python Tool wraps the data returned by Twitter as a TwitterDictResponse object.
		# We convert it back to the JSON format to print/score
		try:
			tweet_str = json.dumps(tweet, indent=4)
			# print(tweet_str)
			num_read+=1
			logger.write_tweet(tweet_str)
			if num_read % batch_size == 0:
				logger.log("Number of tweets read = %s"%str(num_read))
			if num_read == tweet_count:
				break 
		except Exception as e:
			logger.log("Exception occured: %s"%str(e))
			time.sleep(5)
			continue
	logger.log('Need to create another connection!')
	if num_read == tweet_count:
		break 