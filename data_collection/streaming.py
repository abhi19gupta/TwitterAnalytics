try:
    import json
except ImportError:
    import simplejson as json
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
import signal, sys
from datetime import datetime
import time

# ------------------------------------------------------ #

tweet_count = 10000
batch_size = 1000

ACCESS_TOKEN = '894945534410080256-ty8NTmEAUzzwJQTjSAfbmGp81HSVcZb'
ACCESS_SECRET = 'plMGYeenmCZNs7gIDWrO17vEYFrm6GzgZ7BaJdPbMQYuL'
CONSUMER_KEY = 'QC5nVHYoVYdNbl0oQeGExCmWW'
CONSUMER_SECRET = 'e5EUAXqpYSjBZbhEpnqVCMd66WlSJTUJSCUdtQ5dIBmVlWYTIL'
oauth = OAuth(ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
twitter_stream = TwitterStream(auth=oauth)
iterator = twitter_stream.statuses.sample()

def signal_handler(signal, frame):
        log('You pressed Ctrl+C! Number of tweets read = %s'%str(num_read))
        write_to_file(tweet_file,'\n]')
        sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
# ------------------------------------------------------ #

now = str(datetime.now())
tweet_file = ('stream_out_'+now+'.txt').replace(':','-')
log_file = ('log_'+now+'.txt').replace(':','-')

def write_tweets(tweets):
    f = open(tweet_file, 'a')
    for tweet in tweets:
        f.write(',\n'+tweet)
    f.close()

def write_to_file(filename, s):
    f = open(filename, 'a')
    f.write(s)
    f.close()

def log(s):
    to_log = str(datetime.now()) + " | " + s
    print(to_log)
    write_to_file(log_file, to_log+'\n')
# ------------------------------------------------------ #

write_to_file(tweet_file,'[\n')
num_read = 0
tweets = []
for tweet in iterator:
    # Twitter Python Tool wraps the data returned by Twitter as a TwitterDictResponse object.
    # We convert it back to the JSON format to print/score
    try:
        tweet_str = json.dumps(tweet, indent=4)
        # print(tweet_str)
        tweets.append(tweet_str)
        num_read+=1
        if num_read == tweet_count:
            break 
        if num_read % batch_size == 0:
            log("Number of tweets read = %s"%str(num_read))
            write_tweets(tweets)
            tweets = []
    except Exception as e:
        log("Exception occured: %s"%str(e))
        time.sleep(5)
        continue