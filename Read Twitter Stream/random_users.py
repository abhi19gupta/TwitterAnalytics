from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
import json, datetime

f = open('SECRETS','r')
secrets = f.read().split()
f.close()
ACCESS_TOKEN = secrets[0]
ACCESS_SECRET = secrets[1]
CONSUMER_KEY = secrets[2]
CONSUMER_SECRET = secrets[3]
oauth = OAuth(ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
twitter_stream = TwitterStream(auth=oauth)
twitter = Twitter(auth=oauth)

WINDOW_LEN = 17
# each element of list is a pair containing (number of requests made in that 1-minute window, xth minute)
COUNTS= []
LIMIT = 1500

def wait_for_rate_limit():
	while(True):
		curr_minute = int(datetime.datetime.now().timestamp())//60
		while (len(COUNTS)>0 and COUNTS[0][1] < curr_minute-WINDOW_LEN ):
			del(COUNTS[0])
		cnts = sum([x[0] for x in COUNTS])
		if(cnts < LIMIT):
			break
		time.sleep(10) # sleep for 10 seconds
	if(len(COUNTS)>0 and COUNTS[-1][1] == curr_minute):
		COUNTS[-1] = (COUNTS[-1][0]+1,COUNTS[-1][1])
	else:
		COUNTS.append((1,curr_minute))

def my_tweet_fetcher(retry_no=0, **kwargs):
	wait_for_rate_limit()
	return twitter.statuses.user_timeline(**kwargs)
'''
lis = twitter_stream.statuses.sample()
follow_req_needed = 0
users = []
valid_profiles = []
screen_name_dict = {}

for tweet in lis:
	if("user" in tweet):
		if(tweet['user']['screen_name'] not in screen_name_dict):
			screen_name_dict[tweet['user']['screen_name']] = True
			users.append({k:tweet['user'][k] for k in ['id','screen_name','followers_count','friends_count','statuses_count']})
			follow_req_needed+=(tweet['user']['followers_count']//5000+1)
			print(follow_req_needed)
		
	# stop after getting 100 tweets. You can adjust this to any number
	if follow_req_needed >= 15*4*24:
		break

for user in users:
	try:
		my_tweet_fetcher(screen_name=user['screen_name'],count=1)
		valid_profiles.append(user)
	except Exception as e:
		print(type(e),e)
f = open('random_users.txt','a',encoding='utf-8')
f.write(json.dumps(valid_profiles,indent=4))
f.close()
'''
f = open('random_users.txt','r')
f1 = open('users2.txt','w')
users = json.loads(f.read())
for user in users:
	f1.write(user['screen_name'])
	f1.write('\n')
f.close()
f1.close()
