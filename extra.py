# @profile
# driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
# session = driver.session()
# graph = Graph("bolt://localhost:7687",password="password")
# graph.delete_all()
# graph.schema.drop_index("USER","id")
# graph.schema.drop_index("TWEET","id")
# graph.schema.drop_index("HASHTAG","text")
# graph.schema.drop_index("URL","text")

# graph.schema.create_uniqueness_constraint("USER","id")
# graph.schema.create_uniqueness_constraint("TWEET","id")
# graph.schema.create_uniqueness_constraint("HASHTAG","text")
# graph.schema.create_uniqueness_constraint("URL","text")
# graph.schema.create_uniqueness_constraint("TWEET_EVENT","timestamp")
# graph.schema.create_uniqueness_constraint("FRAME","start_t")
# print(graph.schema.get_uniqueness_constraints("FRAME"))
# print(graph.schema.get_uniqueness_constraints("TWEET_EVENT"))
# print(graph.schema.get_uniqueness_constraints("USER"))
# print(graph.schema.get_uniqueness_constraints("TWEET"))
# print(graph.schema.get_uniqueness_constraints("HASHTAG"))
# print(graph.schema.get_uniqueness_constraints("URL"))
def create_tweets(tweet_l):
    run = Node('RUN')
    graph.merge(run)
    count = 0
    tx = graph.begin()
    for tweet in tweet_l:
        count+=1
        print("Creating tweet number ",count)
        retweeted_status      = tweet.get("retweeted_status",None)
        if(retweeted_status is not None):
            continue
        user_id = tweet["user"]["id"]
        tweet['created_at'] = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y').timestamp()

        (frame_start_t, frame_end_t) = getFrameStartEndTime(tweet['created_at'])

        quoted_status         = tweet.get("quoted_status",None)
        in_reply_to_status_id = tweet.get("in_reply_to_status_id",None)
        hashtags    = [x["text"] for x in tweet["entities"]["hashtags"]]
        mention_ids = [x["id"]   for x in tweet["entities"]["user_mentions"]]
        urls        = [{"url": x["url"], "expanded_url": x["expanded_url"]} for x in tweet["entities"]["urls"]]
        flatten_json(tweet)
        # frame = Node("FRAME", start_t=frame_start_t,end_t=frame_end_t)
        # tx.merge(Relationship(run,"HAS_FRAME",frame))
        # tweet_node=Node("TWEET", id=tweet["id"])
        # tx.merge(tweet_node)
        # tweet_node["created_at"] = tweet["created_at"]
        # user_node = Node("USER",id=user_id)
        # tx.merge(user_node)
        # ut_rel = Relationship(user_node,"TWEETED",tweet_node)
        # ut_rel["on"] = tweet["created_at"]
        # tx.create(ut_rel)
        # event_node = Node("TWEET_EVENT",timestamp=tweet["created_at"])
        # fe_rel = Relationship(frame,"HAS_TWEET",event_node)
        # eu_rel = Relationship(event_node,"TE_USER",user_node)
        # et_rel = Relationship(event_node,"TE_TWEET",tweet_node)
        # tx.create(fe_rel)
        # tx.merge(eu_rel)
        # tx.merge(et_rel)
        # for ht in hashtags:
        #     ht_node = Node("HASHTAG",text=ht)
        #     tx.merge(Relationship(tweet_node,"HAS_HASHTAG",ht_node))
        # for url in urls:
        #     url_node = Node("URL",text=url["expanded_url"])
        #     tx.merge(Relationship(tweet_node,"HAS_URL",url_node))
        tx.run(
            "MERGE (run:RUN) "
            "MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
            # Create node for this tweet and frames
            "MERGE (tweet:TWEET {id:{tweet_id}}) " # Maybe the tweet node already partially exists because some other tweet is its reply
            "  ON CREATE SET tweet.created_at = {created_at}, tweet.is_active = true "
            "WITH tweet "
            # Proceed only if the tweet was not already created
            "MATCH (tweet) WHERE NOT (tweet) -[:INFO]-> () "
            # Create user and then the relationships
            "MERGE (user:USER {id:{user_id}}) "
            "CREATE (user) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}), "
            "  (frame) -[:HAS_TWEET]-> (te:TWEET_EVENT {timestamp:{created_at}}), "
            "  (te) -[:TE_USER]-> (user),"
            "  (te) -[:TE_TWEET]-> (tweet) "
            # Create links to hashtags, mentions, urls
            "FOREACH ( hashtag in {hashtags} | "
            "  MERGE (hashtag_node:HASHTAG {text:hashtag}) "
            "  CREATE (tweet) -[:HAS_HASHTAG {on:{created_at}}]-> (hashtag_node) ) "
            "FOREACH ( mention_id in {mention_ids} |  "
            "  MERGE (mention_node:USER {id:mention_id}) "
            "  CREATE (tweet) -[:HAS_MENTION {on:{created_at}}]-> (mention_node) ) "
            "FOREACH ( url in {urls} |  "
            "  MERGE (url_node:URL {url:url.url, expanded_url:url.expanded_url}) "
            "  CREATE (tweet) -[:HAS_URL {on:{created_at}}]-> (url_node) )",
            {"user_id":user_id, "tweet_id":tweet["id"], "created_at":tweet["created_at"] ,"tweet":tweet,
            "hashtags":hashtags, "mention_ids":mention_ids, "urls":urls,
            "frame_start_t":frame_start_t, "frame_end_t":frame_end_t,})

    tx.commit()

# @profile
def create_tweet(tweet, favourited_by=None, fav_timestamp=None):

    # NUM_TWEETS_BUFFERED['value'] += 1

    user_id = tweet["user"]["id"]
    tweet['created_at'] = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y').timestamp()

    (frame_start_t, frame_end_t) = getFrameStartEndTime(tweet['created_at'])
    (fav_frame_start_t, fav_frame_end_t) = (None,None) if fav_timestamp is None else getFrameStartEndTime(fav_timestamp)

    retweeted_status      = tweet.get("retweeted_status",None)
    quoted_status         = tweet.get("quoted_status",None)
    in_reply_to_status_id = tweet.get("in_reply_to_status_id",None)

    if retweeted_status is not None: # in case of retweet, it is better to rely on entities extracted from original tweet
        pass
    else:
        # Extract all requited information before flattening
        hashtags    = [x["text"] for x in tweet["entities"]["hashtags"]]
        mention_ids = [x["id"]   for x in tweet["entities"]["user_mentions"]]
        urls        = [{"url": x["url"], "expanded_url": x["expanded_url"]} for x in tweet["entities"]["urls"]]

        if quoted_status is not None:
            create_tweet(tweet["quoted_status"])

        quoted_status_id = None if quoted_status is None else quoted_status["id"]
        flatten_json(tweet)
        session.run(
            "MERGE (run:RUN) "
            "MERGE (run) -[:HAS_FRAME]-> (frame:FRAME {start_t:{frame_start_t},end_t:{frame_end_t}}) "
            # Create node for this tweet and frames
            "MERGE (tweet:TWEET {id:{tweet_id}}) " # Maybe the tweet node already partially exists because some other tweet is its reply
            "  ON CREATE SET tweet.created_at = {created_at}, tweet.is_active = true "
            # Create favorite relation if needed
            "FOREACH (x IN CASE WHEN {favourited_by} IS NULL THEN [] ELSE [1] END | "
            "  MERGE (run) -[:HAS_FRAME]-> (frame_fav:FRAME {start_t:{fav_frame_start_t},end_t:{fav_frame_end_t}}) "
            "  MERGE (fav_user:USER {id:{favourited_by}}) "
            "  CREATE (fav_user)-[:LIKES {on:{fav_timestamp}}]->(tweet), "
            "    (frame_fav) -[:HAS_FAV]-> (fe:FAV_EVENT {timestamp:{fav_timestamp}}),"
            "    (fe) -[:FAV_USER]-> (fav_user),"
            "    (fe) -[:FAV_TWEET]-> (tweet) )"
            "WITH tweet "
            # Proceed only if the tweet was not already created
            "MATCH (tweet) WHERE NOT (tweet) -[:INFO]-> () "
            # Create user and then the relationships
            "MERGE (user:USER {id:{user_id}}) "
            "CREATE (user) -[:TWEETED {on:{created_at}}]-> (tweet) -[:INFO]-> (:TWEET_INFO {tweet}), "
            "  (frame) -[:HAS_TWEET]-> (te:TWEET_EVENT {timestamp:{created_at}}), "
            "  (te) -[:TE_USER]-> (user),"
            "  (te) -[:TE_TWEET]-> (tweet) "
            # Create links to hashtags, mentions, urls
            "FOREACH ( hashtag in {hashtags} | "
            "  MERGE (hashtag_node:HASHTAG {text:hashtag}) "
            "  CREATE (tweet) -[:HAS_HASHTAG {on:{created_at}}]-> (hashtag_node) ) "
            "FOREACH ( mention_id in {mention_ids} |  "
            "  MERGE (mention_node:USER {id:mention_id}) "
            "  CREATE (tweet) -[:HAS_MENTION {on:{created_at}}]-> (mention_node) ) "
            "FOREACH ( url in {urls} |  "
            "  MERGE (url_node:URL {url:url.url, expanded_url:url.expanded_url}) "
            "  CREATE (tweet) -[:HAS_URL {on:{created_at}}]-> (url_node) )"
            # Create link to quoted tweet in case this tweet quotes another tweet
            "FOREACH (x IN CASE WHEN {quoted_status_id} IS NULL THEN [] ELSE [1] END | "
            "  MERGE (quoted_tweet:TWEET {id:{quoted_status_id}}) "
            "  CREATE (tweet) -[:QUOTED {on:{created_at}}]-> (quoted_tweet) )"
            # Create link to original tweet in case this is a reply tweet
            "FOREACH (x IN CASE WHEN {in_reply_to_status_id} IS NULL THEN [] ELSE [1] END | "
            "  MERGE (in_reply_to_tweet:TWEET {id:{in_reply_to_status_id}}) "
            "  CREATE (tweet) -[:REPLY_TO {on:{created_at}}]-> (in_reply_to_tweet) )",
            {"user_id":user_id, "tweet_id":tweet["id"], "created_at":tweet["created_at"] ,"tweet":tweet,
            "hashtags":hashtags, "mention_ids":mention_ids, "urls":urls,
            "quoted_status":quoted_status, "quoted_status_id":quoted_status_id,
            "in_reply_to_status_id": in_reply_to_status_id, "frame_start_t":frame_start_t, "frame_end_t":frame_end_t,
            "favourited_by":favourited_by, "fav_timestamp":fav_timestamp, "fav_frame_start_t":fav_frame_start_t,
            "fav_frame_end_t":fav_frame_end_t})
