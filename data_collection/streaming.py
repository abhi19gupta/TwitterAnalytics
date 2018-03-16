try:
    import json
except ImportError:
    import simplejson as json
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
import signal, sys
from datetime import datetime
import time

now = str(datetime.now())
tweet_file = ('stream_out_'+now+'.txt').replace(':','-')
log_file = ('log_'+now+'.txt').replace(':','-')

def write_to_file(filename, s):
    f = open(filename, 'a')
    f.write(s)
    f.close()

def log(s):
    to_log = str(datetime.now()) + " | " + s
    print(to_log)
    write_to_file(log_file, to_log+'\n')

ACCESS_TOKEN = '894945534410080256-ty8NTmEAUzzwJQTjSAfbmGp81HSVcZb'
ACCESS_SECRET = 'plMGYeenmCZNs7gIDWrO17vEYFrm6GzgZ7BaJdPbMQYuL'
CONSUMER_KEY = 'QC5nVHYoVYdNbl0oQeGExCmWW'
CONSUMER_SECRET = 'e5EUAXqpYSjBZbhEpnqVCMd66WlSJTUJSCUdtQ5dIBmVlWYTIL'

oauth = OAuth(ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
twitter_stream = TwitterStream(auth=oauth)
iterator = twitter_stream.statuses.sample()

tweet_count = 1000
num_read = 0

write_to_file(tweet_file,'[\n')

def signal_handler(signal, frame):
        log('You pressed Ctrl+C! Number of tweets read = %s'%str(num_read))
        write_to_file(tweet_file,'\n]')
        sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

for tweet in iterator:
    # Twitter Python Tool wraps the data returned by Twitter as a TwitterDictResponse object.
    # We convert it back to the JSON format to print/score
    try:
        tweet_str = json.dumps(tweet, indent=4)
        # print(tweet_str)
        if (num_read != 0):
            write_to_file(tweet_file,',\n')
        write_to_file(tweet_file,tweet_str)
        num_read+=1
        if num_read == tweet_count:
            break 
        if num_read % 100 == 0:
            log("Number of tweets read = %s"%str(num_read))
    except Exception as e:
        log("Exception occured: %s"%str(e))
        time.sleep(5)
        continue