from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime


driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
session = driver.session()


'''
USER NETWORK
    Node labels - USER(id,screen_name), USER_INFO(dict)
    Relationships - CURR_STATE(from), INTITIAL_STATE(on), PREV(from,to)
FOLLOWER NETWORK
    Node labels -
    Relationships - FOLLOWS(from,to), FOLLOWED(from,to) // FOLLOWS.to will always be the last time data was collected
TWEET NETWORK
    Node labels - TWEET(id,created_at,is_active), TWEET_INFO(dict), HASHTAG(text), URL(url,expanded_url), //MEDIA(url, media_url)//, PLACE(id,name,country) -> This is not the location of tweet but the location with which the tweet is tagged (could be about it)
    Relationships - TWEETED(on), INFO, REPLY_TO(on), RETWEET_OF(on), QUOTED(on), HAS_MENTION(on), HAS_HASHTAG(on), //HAS_MEDIA(on)//, HAS_URL(on), HAS_PLACE(on)
TIME NODES
    Node labels - FRAME(start time, end time)
    Relationships TWEET_EVENT(timestamp) LIKE_EVENT(timestamp) FOLLOW_EVENT(timestamp)
EVENT NODES
    TWEET_EVENT
    FOLLOW_EVENT
    LIKE_EVENT
    Relationships : TWEET_EVENT TWEET_USER TWEET_TWEET(from TWEET_EVENT to USER and TWEET nodes)
    Relationships : LIKE_EVENT LIKES LIKED(from LIKE_EVENT to TWEET nodes)
    Relationships : FOLLOW_EVENT FOLLOWS FOLLOWED(from FOLLOW_EVENT to USER nodes)
RUN : is the main node(root) under which the complete graph resides
RUN has LAST_FRAME HAS_FRAME PREV to FRAME node
'''
delta = 5
initial_time = 2

def create_user(id, screen_name, user_info_dict, timestamp):
    alreadyExists = session.run(
        "MATCH (user:USER {id:{id}}) -[:CURR_STATE]-> () "
        "RETURN user",
        {"id":id})
    if len(list(alreadyExists)) > 0:
        print("create_user: User [id:", id,"] already exists! Aborting.")
        return
    results = session.run(
        "MERGE (user:USER {id:{id}}) " # not creating directly in case node already exists because of tweet network
        "SET user.screen_name = {screen_name} "
        "CREATE (user) -[:CURR_STATE {from:{now}}]-> (state:USER_INFO {user_info_dict}), "
        "(user) -[:INITIAL_STATE {on:{now}}]-> (state) "
        "RETURN user,state",
        {"id":id, "screen_name":screen_name, "user_info_dict":user_info_dict, "now":timestamp})


def create_tweet_event(user_id, time_of_creation, tweet_id):
    frame_start_time = (time_of_creation-initial_time)//delta;
    results = session.run(
        "MERGE (run :RUN)"
        "MERGE (run) -[edge :HAS_FRAME {start_time:{frame_start_time}}]-> (frame :FRAME)"
        'ON CREATE SET run.label = "RUN", edge.label = "HAS_EDGE", frame.label = "FRAME"'
        "CREATE (frame) -[:TE_EDGE {time_stamp:{now}}]-> (te :TWEET_EVENT),"
        "(te) -[:TE_USER]-> (u :USER {id:{user_id}}),"
        "(te) -[:TE_TWEET]-> (t:TWEET {id:{tweet_id}})"
        'SET te.label = "TWEET_EVENT"'
        "RETURN u.id,t.id",
        {"frame_start_time":frame_start_time,"user_id":user_id,"tweet_id":tweet_id,"now":time_of_creation}
    )
    for result in results:
        print(result)
def update_followers(user_id, follower_ids, timestamp):
    frame_start_time = (timestamp-initial_time)//delta;
    # First, for all followers in argument, either create FOLLOWS or update the "to" field of FOLLOWS to current timestamp
    session.run(
        "MATCH (user:USER {id:{user_id}}) "
        "UNWIND {follower_ids} AS follower_id "
        "MERGE (run :RUN) "
        "MERGE (run) -[edge :HAS_FRAME {start_time:{frame_start_time}}]-> (frame :FRAME) "
        'ON CREATE SET run.label = "RUN", edge.label = "HAS_EDGE", frame.label = "FRAME" '
        "MERGE (follower:USER {id:follower_id}) " # keep this merge separate from below o/w multiple nodes can be created
        "MERGE (user) <-[follows_rel:FOLLOWS]- (follower) "
        "ON CREATE SET follows_rel.made = 1 "
        "ON MATCH SET follows_rel.to = {now} "
        "FOREACH (x IN CASE WHEN follows_rel.made=1 THEN [1] ELSE [] END | "
        "SET follows_rel.from = {now}, follows_rel.to = {now}) "
        'FOREACH (x IN CASE WHEN follows_rel.made=1 THEN [1] ELSE [] END | CREATE (frame) -[:FE_EDGE {time_stamp:{now}}]-> (fe :FOLLOW_EVENT {label:"FOLLOW_EVENT"}), '
        "(fe) -[:FE_FOLLOWED]-> (user), "
        '(fe) -[:FE_FOLLOWS]-> (follower)) '
        "REMOVE follows_rel.made ",
        {"user_id":user_id, "follower_ids":follower_ids, "now":timestamp, "frame_start_time":frame_start_time})
    # Now, for all FOLLOWS whose "to" field is not current timestamp, make them FOLLOWED
    session.run(
        "MATCH (user:USER {id:{user_id}}) <-[follows_rel:FOLLOWS]- (follower:USER) "
        "WHERE follows_rel.to <> {now} "
        "CREATE (user) <-[:FOLLOWED {from:follows_rel.from, to:follows_rel.to}]- (follower) "
        "DELETE follows_rel", # Can change these 2 statements to a single SET statement
        {"user_id":user_id, "now":timestamp})


def clear_db():
    session.run("MATCH (n) DETACH DELETE n")
    for index in session.run("CALL db.indexes()"):
        session.run("DROP "+index["description"])

clear_db()
create_user(1,"Deepak",{"m1":"d1","m2":"d2"},1)
create_user(2,"Abhishek",{"m1":"d1","m2":"d2"},2)
create_user(3,"Aman",{"m1":"d1","m2":"d2"},3)
create_user(4,"Manoj",{"m1":"d1","m2":"d2"},2)

update_followers(1,[3],5)
update_followers(1,[3],8)
update_followers(1,[3,4],9)

session.close()



