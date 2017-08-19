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

import json, datetime, os, shutil
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
from pymongo import MongoClient


# DATABASE RELATED SETUP
client = MongoClient(port=27017)
db=client.twitter # db name

# TWITTER API RELATED SETUP
ACCESS_TOKEN = '894945534410080256-ty8NTmEAUzzwJQTjSAfbmGp81HSVcZb'
ACCESS_SECRET = 'plMGYeenmCZNs7gIDWrO17vEYFrm6GzgZ7BaJdPbMQYuL'
CONSUMER_KEY = 'QC5nVHYoVYdNbl0oQeGExCmWW'
CONSUMER_SECRET = 'e5EUAXqpYSjBZbhEpnqVCMd66WlSJTUJSCUdtQ5dIBmVlWYTIL'
oauth = OAuth(ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
twitter = Twitter(auth=oauth)

# DATABASE SCHEMA
user_fields = ['created_at','description','entities','favourites_count','followers_count','friends_count','id','id_str',
'listed_count','location','name','profile_image_url','protected','screen_name','statuses_count','time_zone','url',
'verified','withheld_in_countries','withheld_scope']

tweet_fields = ['coordinates','created_at','entities','favorite_count','id','id_str','place','possibly_sensitive',
'retweet_count','retweeted_status','text','user','in_reply_to_screen_name','in_reply_to_status_id_str',
'in_reply_to_user_id_str','withheld_copyright','withheld_in_countries','withheld_scope']

# Default json encoder is not able to serialize datetime
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)

# FETCH AND PERSIST USER INFORMATION (calling this multiple times will keep adding new entries so that you can compare over time)
def fetch_persist_users(user_screen_names):

	def get_data_to_persist(user_info,time):
		ret = {k:user_info.get(k,None) for k in user_fields}
		ret['record_creation_date'] = time
		return ret

	users_info = twitter.users.lookup(screen_name=user_screen_names)
	time = datetime.datetime.now()
	time_str = str(time).replace(":","-")
	for user_info in users_info:
		screen_name = user_info['screen_name']
		f = open('user_info/'+screen_name+"_"+time_str+'.txt', 'w')
		f.write(json.dumps(user_info,indent=4,cls=DateTimeEncoder))
		f.close()
		user_info_to_persist = get_data_to_persist(user_info,time)
		f = open('user_info_persisted/'+screen_name+"_"+time_str+'.txt', 'w')
		f.write(json.dumps(user_info_to_persist,indent=4,cls=DateTimeEncoder))
		f.close()
		db.users.insert_one(user_info_to_persist)
	print("Done with user info")

# FETCH AND PERSIST TWEET INFORMATION (EXCLUDING TWEETS ALREADY PERSISTED)
def fetch_persist_tweets(user_screen_names):

	def get_data_to_persist(tweets,time):
		ret = []
		for tweet in tweets:
			tweet_ = {k:tweet.get(k,None) for k in tweet_fields}
			tweet_['record_creation_date'] = time
			ret.append(tweet_)
		return ret

	for screen_name in user_screen_names:
		print('Fetching tweets for '+screen_name)
		time = datetime.datetime.now()
		time_str = str(time).replace(":","-")
		f = open('tweets/'+screen_name+"_"+time_str+".txt", 'w')
		f1 = open('tweets_persisted/'+screen_name+"_"+time_str+".txt", 'w')
		# Find the tweet id beyond which to fetch new tweets
		since_id_query = db.max_ids.find_one({'screen_name':screen_name})
		since_id = 1 if since_id_query is None else since_id_query['max_id']
		tweets = twitter.statuses.user_timeline(screen_name=screen_name,count=200,trim_user='true',
			include_rts='true',exclude_replies='false',since_id=since_id)
		next_since_id = since_id if len(tweets)==0 else max([tweet['id'] for tweet in tweets])
		db.max_ids.update_one({'screen_name':screen_name},{'$set':{'max_id':next_since_id}},upsert=True)
		while(len(tweets)!=0):
			print('\tFetched '+str(len(tweets)))
			f.write(json.dumps(tweets,indent=4,cls=DateTimeEncoder))
			f.write('\n')
			tweets_to_persist = get_data_to_persist(tweets,time)
			f1.write(json.dumps(tweets_to_persist,indent=4,cls=DateTimeEncoder))
			f1.write('\n')
			min_id = min([tweet['id'] for tweet in tweets])
			tweets = twitter.statuses.user_timeline(screen_name=screen_name,count=200,trim_user='true',
				include_rts='true',exclude_replies='false',max_id=min_id-1,since_id=since_id)
			db.tweets.insert_many(tweets_to_persist)
		f.close()
		f1.close()
	print("Done with tweets")

def clear_everyting():
	def clear_folder(folder_name):
		for the_file in os.listdir(folder_name):
		    file_path = os.path.join(folder_name, the_file)
		    try:
		        if os.path.isfile(file_path):
		            os.unlink(file_path)
		    except Exception as e:
		        print(e)
		
	db.users.drop()
	db.tweets.drop()
	for folder_name in ['user_info','user_info_persisted','tweets','tweets_persisted']:
		clear_folder(folder_name)


# clear_everyting()
user_screen_names = ['elonmusk','narendramodi','BillGates','iamsrk','imVkohli']
# fetch_persist_users(','.join(user_screen_names))
fetch_persist_tweets(user_screen_names)