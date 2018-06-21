"""
Module to stream 1% sample of tweets using Twitter's Streaming API. The code will write the data in the same directory.

Create a file named SECRETS which contains your Twitter OAuth related keys in the following order, separated by new lines:
<ACCESS_TOKEN>
<ACCESS_SECRET>
<CONSUMER_KEY>
<CONSUMER_SECRET>

**Running the code**:

    * First ensure Python Twitter Tools is installed. (https://github.com/sixohsix/twitter)
    * Before running, you may set the following parameters inside the file:

        * tweet_count = 1000000000 # total number of tweets to read
        * batch_size = 10000 # how many tweets to write together in 1 go
        * file_tweet_count = 500000 # max no of tweets to write in 1 file

  *Command to run*:
  ``python streaming.py``

"""

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
	'''
	Class to periodically write tweets to files and to log anything (automatically timestamped).
	The function write_tweet will keep buffering tweets till batch_size is reached. Once the batch size
	is reached, it flushes all the buffered tweets to the current tweet_file_name. Additionally, it keeps
	track of number of tweets written to the current file and once it exceeds the threshold file_tweet_count,
	it starts with a new tweet_file and starts flushing to it. This will prevent a single file from becoming
	too big in size.
	'''
	def __init__(self, batch_size, file_tweet_count):
		self.batch_size = batch_size
		self.file_tweet_count = file_tweet_count
		self.unwritten_tweets = []
		self.configure_new_file()

	'''
	Changes the configuration to start logging and writing tweets to new files (filenames will contain timestamp).
	Also puts a '[' at the beginning of the tweet file to start a new list.
	'''
	def configure_new_file(self):
		now = str(datetime.now())
		self.tweet_file_name = ('stream_out_%s.txt'%(now)).replace(':','-')
		self.log_file_name = ('log_%s.txt'%(now)).replace(':','-')
		self.current_file_count = 0
		with open(self.tweet_file_name, 'a') as f:
			f.write('[\n')

	'''
	Finshes the current tweet file by ending the list with a ']'.
	'''
	def finish_current_file(self):
		with open(self.tweet_file_name, 'a') as f:
			f.write('\n]')

	'''
	Logs the string supplied as argument to the current log file.

	:param s: The string to log
	'''
	def log(self, s):
		to_log = str(datetime.now()) + " | " + s + '\n'
		print(to_log)
		with open(self.log_file_name, 'a') as f:
			f.write(to_log)

	'''
	Adds the tweet to the buffer. If buffer is filled, then flushes the buffer to the current tweet file.
	If the current tweet file exceeds the tweet count threshold, finishes the current file and a configures a new one.

	:param tweet: The tweet to write to file
	'''
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
logger = Logger(batch_size, file_tweet_count)

# TWITTER API RELATED SETUP
# f = open('SECRETS','r')
# secrets = f.read().split()
# f.close()
# ACCESS_TOKEN = secrets[0]
# ACCESS_SECRET = secrets[1]
# CONSUMER_KEY = secrets[2]
# CONSUMER_SECRET = secrets[3]
# oauth = OAuth(ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
# twitter = Twitter(auth=oauth)

def signal_handler(signal, frame):
		logger.log('You pressed Ctrl+C! Number of tweets read = %s'%str(num_read))
		logger.finish_current_file()
		sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
# ------------------------------------------------------ #

if __name__ == '__main__':
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
