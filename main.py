# Before running on windows, run the following:
# set http_proxy=proxy62.iitd.ac.in:3128
# set https_proxy=proxy62.iitd.ac.in:3128

# Starting mongo process
# 	mongod --dbpath "C:\mymongodata"

# To connect to this process
# 	mongo
# 	show dbs
# 	use twitter
# 	db.tweets.count()
# 	https://docs.mongodb.com/manual/reference/mongo-shell/

#### CREATE #####
# business = {
#     'name' : 'QD',
#     'rating' : 10,
#     'cuisine' : 'Italian' 
# }
# result=db.reviews.insert_one(business)

#### RETRIEVE ####
# db.reviews.find_one({'rating': 5})
# db.reviews.count()
# db.reviews.find({'rating': 5}).count()
# for post in posts.find({"date": {"$lt": datetime.datetime(2009, 11, 12, 12)}}).sort("author"):
# for review in review.find():

#### UPDATE ####
# db.reviews.update_one({'_id' : ASingleReview.get('_id') }, {'$inc': {'likes': 1}}) # If 'likes is not there originally, it will be added

#### DELETE ####
# db.restaurants.delete_many({“category”: “Bar Food“})

#### Indexing ####
# db.profiles.create_index([('user_id', pymongo.ASCENDING)], unique=True)

import json, datetime, os, shutil, pymongo, operator, time
import matplotlib.pyplot as plt
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
from pymongo import MongoClient


# DATABASE RELATED SETUP
# client = MongoClient(port=27017)
# db=client.twitter # db name

# TWITTER API RELATED SETUP
ACCESS_TOKEN = '894945534410080256-ty8NTmEAUzzwJQTjSAfbmGp81HSVcZb'
ACCESS_SECRET = 'plMGYeenmCZNs7gIDWrO17vEYFrm6GzgZ7BaJdPbMQYuL'
CONSUMER_KEY = 'QC5nVHYoVYdNbl0oQeGExCmWW'
CONSUMER_SECRET = 'e5EUAXqpYSjBZbhEpnqVCMd66WlSJTUJSCUdtQ5dIBmVlWYTIL'
oauth = OAuth(ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
twitter = Twitter(auth=oauth)
'''
# DATABASE SCHEMA
user_fields = ['created_at','description','entities','favourites_count','followers_count','friends_count','id','id_str',
'listed_count','location','name','profile_image_url','protected','screen_name','statuses_count','time_zone','url',
'verified','withheld_in_countries','withheld_scope']

tweet_fields = ['coordinates','created_at','entities','favorite_count','id','id_str','place','possibly_sensitive',
'retweet_count','retweeted_status','text','user','in_reply_to_screen_name','in_reply_to_status_id_str',
'in_reply_to_user_id_str','withheld_copyright','withheld_in_countries','withheld_scope']
'''
# Default json encoder is not able to serialize datetime
class DateTimeEncoder(json.JSONEncoder):
	def default(self, o):
		if isinstance(o, datetime.datetime):
			return o.isoformat()

		return json.JSONEncoder.default(self, o)

def init():
	prefix = os.path.dirname(os.path.realpath(__file__))+'/data/'
	dirs = ['user_info/','tweet_max_ids/','tweets/','user_followers/','user_friends/','fav_max_ids/','favourites/']
	for dir_ in dirs:
		if (not os.path.exists(prefix+dir_)):
			os.makedirs(prefix+dir_)

################### TWITTER API FUNCTIONS HERE ####################

WINDOW_LEN = 17
# each element of each list is a pair containing (number of requests made in that 1-minute window, xth minute)
COUNTS_DICT = {'USERS_LOOKUP':[], 'TWEETS':[], 'FOLLOWERS':[], 'FRIENDS':[], 'FAVOURITES':[]}
COUNTS_LIMIT = {'USERS_LOOKUP':300, 'TWEETS':1500, 'FOLLOWERS':15, 'FRIENDS':15, 'FAVOURITES':75}

def wait_for_rate_limit(type_):
	COUNTS = COUNTS_DICT[type_]
	LIMIT = COUNTS_LIMIT[type_]
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

def my_user_fetcher(**kwargs):
	wait_for_rate_limit('USERS_LOOKUP')
	return twitter.users.lookup(**kwargs)

def my_tweet_fetcher(**kwargs):
	wait_for_rate_limit('TWEETS')
	return twitter.statuses.user_timeline(**kwargs)

def my_followers_fetcher(**kwargs):
	wait_for_rate_limit('FOLLOWERS')
	return twitter.followers.ids(**kwargs)

def my_friends_fetcher(**kwargs):
	wait_for_rate_limit('FRIENDS')
	return twitter.friends.ids(**kwargs)

def my_favourites_fetcher(**kwargs):
	wait_for_rate_limit('FAVOURITES')
	return twitter.favorites.list(**kwargs)
	
# ************************************************

# FETCH AND PERSIST USER INFORMATION (calling this multiple times will keep adding new entries so that you can compare over time)
# 300 requests per 15 min, 100 users per request = 30,000 per 15 min
def fetch_persist_users(user_screen_names, time): 

	def get_data_to_persist(user_info,time):
		ret = user_info
		# ret = {k:user_info.get(k,None) for k in user_fields}
		ret['record_creation_date'] = time
		# if 'created_at' in user_fields:
		ret['created_at'] = datetime.datetime.strptime(ret['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
		return ret

	print('Fetching user info')
	time_str = str(time).replace(":","-")
	for i in range(0,len(user_screen_names),100):
		print('\tUsers %d to %d'%(i,min(i+99,len(user_screen_names)-1)))
		curr_users = ','.join(user_screen_names[i:i+100]) # atmost 100 users in a request
		users_info = my_user_fetcher(screen_name=curr_users)
		for user_info in users_info:
			screen_name = user_info['screen_name']
			f = open('data/user_info/'+screen_name+"_"+time_str+'.txt', 'w')
			f.write(json.dumps(user_info,indent=4,cls=DateTimeEncoder))
			f.close()
			# user_info_to_persist = get_data_to_persist(user_info,time)
			# f = open('user_info_persisted/'+screen_name+"_"+time_str+'.txt', 'w')
			# f.write(json.dumps(user_info_to_persist,indent=4,cls=DateTimeEncoder))
			# f.close()
			# db.users.insert_one(user_info_to_persist)
	print("Done with user info")

# FETCH AND PERSIST TWEET INFORMATION (EXCLUDING TWEETS ALREADY PERSISTED)
# 1500 requests per 15 min, 200 tweets returned per request = 3,00,000 per 15 min, 3200 per user
def fetch_persist_tweets(user_screen_names,time,type_):
	
	assert(type_=='tweets' or type_=='favourites') 
	if(type_=='tweets'):
		MAX_IDS_FOLDER = 'data/tweet_max_ids/'
		DATA_FOLDER = 'data/tweets/'
	else:
		MAX_IDS_FOLDER = 'data/fav_max_ids/'
		DATA_FOLDER = 'data/favourites/'

	def get_data_to_persist(tweets,time):
		ret = []
		for tweet in tweets:
			tweet_ = tweet
			# tweet_ = {k:tweet.get(k,None) for k in tweet_fields}
			tweet_['record_creation_date'] = time
			# if 'created_at' in tweet_fields:
			tweet_['created_at'] = datetime.datetime.strptime(tweet_['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
			ret.append(tweet_)
		return ret

	def getMaxId(screen_name):
		# since_id_query = db.max_ids.find_one({'screen_name':screen_name})
		# since_id = 1 if since_id_query is None else since_id_query['max_id']
		since_id = 1
		if (os.path.exists(os.path.dirname(os.path.realpath(__file__)) + '/'+ MAX_IDS_FOLDER + screen_name + ".txt")):
			f_max_id = open(MAX_IDS_FOLDER + screen_name + ".txt", 'r')
			since_id = int(f_max_id.read())
			f_max_id.close()
		return since_id

	def persistMaxId(screen_name,max_id):
		# db.max_ids.update_one({'screen_name':screen_name},{'$set':{'max_id':next_since_id}},upsert=True)
		f_max_id = open(MAX_IDS_FOLDER + screen_name + ".txt", 'w')
		f_max_id.write(str(max_id))
		f_max_id.close()

	for screen_name in user_screen_names:
		print('Fetching %s for %s'%(type_,screen_name))
		time_str = str(time).replace(":","-")
		f = open(DATA_FOLDER+screen_name+"_"+time_str+".txt", 'w')
		f.write('[\n') # creating a list of list
		# f1 = open('tweets_persisted/'+screen_name+"_"+time_str+".txt", 'w')
		# Find the tweet id beyond which to fetch new tweets
		since_id = getMaxId(screen_name)
		if(type_=='tweets'):
			tweets = my_tweet_fetcher(screen_name=screen_name,count=200,trim_user='true',
				include_rts='true',exclude_replies='false',since_id=since_id)
		else:
			tweets = my_favourites_fetcher(screen_name=screen_name,count=200,since_id=since_id)
		next_since_id = since_id if len(tweets)==0 else max([tweet['id'] for tweet in tweets])
		persistMaxId(screen_name,next_since_id)
		while(len(tweets)!=0):
			print('\tFetched '+str(len(tweets)))
			f.write(json.dumps(tweets,indent=4,cls=DateTimeEncoder))
			f.write(',\n')
			# tweets_to_persist = get_data_to_persist(tweets,time)
			# f1.write(json.dumps(tweets_to_persist,indent=4,cls=DateTimeEncoder))
			# f1.write('\n')
			min_id = min([tweet['id'] for tweet in tweets])
			if(type_=='tweets'):
				tweets = my_tweet_fetcher(screen_name=screen_name,count=200,trim_user='true',
					include_rts='true',exclude_replies='false',max_id=min_id-1,since_id=since_id)
			else:
				tweets = my_favourites_fetcher(screen_name=screen_name,count=200,max_id=min_id-1,since_id=since_id)
			# db.tweets.insert_many(tweets_to_persist)
		f.write('[]]') # adding an empty list at the end because of the last comma
		f.close()
		# f1.close()
	print("Done with %s"%(type_))

# followers: 15 requests per 15 min, 5000 followers returned per request (same for friends); 75000 per 15 min
def fetch_persist_friends_and_followers(user_screen_names,time):

	def get_existing(screen_name, type_):
		assert(type_=='followers' or type_=='friends')
		folder_name = 'data/user_followers' if type_=='followers' else 'data/user_friends'
		ret = {}
		for the_file in os.listdir(folder_name):
			if(the_file.startswith(screen_name)):
				file_path = os.path.join(folder_name, the_file)
				f = open(file_path,'r')
				ls = json.loads(f.read())
				for x in ls:
					ret[x] = True
				f.close()
		return ret

	# returns the number of new followers/friends in this batch
	def get_new_count(existing, current_batch):
		if (current_batch[-1] in existing):
			if (current_batch[0] in existing):
				return 0
			# do binary search (first not in existing, last in existing, point of transition in between)
			first = 0
			last = len(current_batch)-1
			while (first < last): # (= shall never happen)
				mid = (first+last)//2
				if (mid==first): # first=last-1
					return first+1
				elif (current_batch[mid] in existing):
					last = mid
				else:
					first = mid
			print("Should not reach here")
		else:
			return len(current_batch)

	def fetch_and_store(screen_name, type_):
		print('Fetching %s for %s'%(type_, screen_name))
		time_str = str(time).replace(":","-")
		existing = get_existing(screen_name, type_)
		new = []
		cursor = -1
		func = my_followers_fetcher if type_=='followers' else my_friends_fetcher
		while (True):
			api_res = func(screen_name=screen_name, cursor=cursor)
			batch = api_res['ids']
			print('\tBatch:', str(len(batch)), str(batch[:1]))
			cursor = api_res['next_cursor']
			new_count = get_new_count(existing, batch)
			new.extend(batch[:new_count])
			if(new_count < len(batch) or cursor == 0):
				break
		print('\tTotal = '+str(len(new)))
		folder_name = 'data/user_followers/' if type_=='followers' else 'data/user_friends/'
		f = open(folder_name+screen_name+"_"+time_str+'.txt', 'w')
		f.write(json.dumps(new))
		f.close()

	for screen_name in user_screen_names:
		fetch_and_store(screen_name, 'followers')
		fetch_and_store(screen_name, 'friends')
	print('Done with followers/friends')


def clear_everyting():
	def clear_folder(folder_name):
		for the_file in os.listdir(folder_name):
			file_path = os.path.join(folder_name, the_file)
			try:
				if os.path.isfile(file_path):
					os.unlink(file_path)
			except Exception as e:
				print(e)
		
	# db.users.drop()
	# db.tweets.drop()
	# db.max_ids.drop()
	folders = ['data/tweet_max_ids','data/tweets','data/tweets_persisted','data/user_followers','data/user_friends',
	'data/user_info','data/user_info_persisted','data/fav_max_ids','data/favourites']
	for folder_name in folders:
		clear_folder(folder_name)


################### ANALYTICS PART BEGINS HERE ####################
def plot_user_field(screen_name,date_start,date_end,field_names):
	results = list(db.users.find(filter={'screen_name':screen_name, 'record_creation_date':{'$gt':date_start,'$lt':date_end}},
		projection=field_names+['record_creation_date'],sort=[('record_creation_date', pymongo.ASCENDING)]))
	xVals = [x['record_creation_date'] for x in results]
	print(xVals)
	for field_name in field_names:
		yVals = [x[field_name] for x in results]
		print(yVals)
		plt.plot(xVals,yVals,'bo-')
		plt.xlabel('DateTime')
		plt.ylabel(field_name)
		plt.title(date_start.strftime("%d-%m-%Y %H:%M:%S")+' to '+date_end.strftime("%d-%m-%Y %H:%M:%S"))
		plt.margins(0.1) # Pad margins so that markers don't get clipped by the axes
		plt.subplots_adjust(bottom=0.15) # Tweak spacing to prevent clipping of tick-labels
		# plt.xticks(xVals,xVals,rotation=30)
		plt.savefig('plots/'+screen_name+'_'+field_name+'.png')
		# plt.show()
		plt.clf()

def extract_hash_tags(screen_name,date_start,date_end):
	id_ = db.users.find_one({'screen_name':screen_name})['id']
	query_result = list(db.tweets.find(filter={'user.id':id_, 'created_at':{'$gt':date_start,'$lt':date_end}},
		projection=['entities.hashtags']))
	hashtag_counts = {}
	for tweet in query_result:
		for hashtag in tweet['entities']['hashtags']:
			hashtag = hashtag['text']
			hashtag_counts[hashtag] = hashtag_counts.get(hashtag,0) + 1
	print('Hashtags used by '+screen_name)
	print(sorted(hashtag_counts.items(), key=operator.itemgetter(1), reverse=True))

################### MAIN FUNCTION BEGINS HERE ####################
init()
now = datetime.datetime.now()

# clear_everyting()
with open('data/timestamps.txt','a') as f:
	f.write(str(now).replace(":","-")+'\n')

user_screen_names = ['elonmusk','narendramodi','BillGates','iamsrk','imVkohli']
fetch_persist_users(user_screen_names,now)
fetch_persist_tweets(user_screen_names,now,'tweets')
fetch_persist_tweets(user_screen_names,now,'favourites')
fetch_persist_friends_and_followers(user_screen_names,now)

# plot_user_field('narendramodi',now-datetime.timedelta(days=1),now,
# 	['favourites_count','followers_count','friends_count','statuses_count'])
# extract_hash_tags('iamsrk',now-datetime.timedelta(days=100),now)
