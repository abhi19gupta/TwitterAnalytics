# TO VIEW ALL NODES:
# MATCH (n) RETURN n

# TO CLEAR THE DATABSE:
# MATCH (a)-[r]->(b) DELETE r
# MATCH (a) DELETE a

from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
session = driver.session()

INFI_TIME = 1000000000000

'''
USER NW
	Node labels - USER(id,id_str,screen_name), USER_INFO(dict)
	Relationships - CURR_STATE(from), INTITIAL_STATE(on), PREV(from,to)

FOLLOWER NETWORK
	Node labels - 
	Relationships - FOLLOWS(from,to), FOLLOWED(from,to) // FOLLOWS.to will always be the last time data was collected
'''

# CAN MAKE THE FOLLOWING CHANGE - For user_info no need of TO and FROM, just keep ON because we create a new node everytime and we have information only of that timestamp.
def create_user(id,id_str,screen_name,user_info_dict,timestamp):
	results = session.run(
		"CREATE (user:USER {id: {id}, id_str: {id_str}, screen_name: {screen_name}}) "
		"-[:CURR_STATE {from:{now}}]-> "
		"(state:USER_INFO {user_info_dict}), "
		"(user) -[:INITIAL_STATE {on:{now}}]-> (state) "
		"RETURN user,state",
		{"id":id, "id_str":id_str, "screen_name":screen_name, "user_info_dict":user_info_dict, "now":timestamp})
	for result in results:
		print(result["user"])

def add_user_info_to_linked_list(screen_name,user_info_dict,timestamp):
	session.run(
		# Don't do the following, it will match nothing if there is no current state
		# "MATCH (user:User {screen_name:{screen_name}}) -[curr_state:CURR_STATE]-> (prev_user_info:USER_INFO) "
		"MATCH (user:USER {screen_name:{screen_name}})"
		"CREATE (user) -[:CURR_STATE {from:{now}}]-> (curr_user_info:USER_INFO {user_info_dict}) "
		"WITH user, curr_user_info "
		"MATCH (curr_user_info) <-[:CURR_STATE]- (user) -[prev_state_rel:CURR_STATE]-> (prev_user_info) "
		"CREATE (curr_user_info) -[:PREV {from:prev_state_rel.from, to:{now}}]-> (prev_user_info)"
		"DELETE prev_state_rel ",
		{"screen_name":screen_name, "user_info_dict":user_info_dict, "now":timestamp})

def update_followers(screen_name, follower_ids_str,timestamp):
	# First, for all followers in argument, either create FOLLOWS or update the "to" field of FOLLOWS to current timestamp
	session.run(
		"MATCH (user:USER {screen_name:{screen_name}}) "
		"UNWIND {follower_ids_str} AS follower_id_str "
		"MERGE (follower:USER {id_str:follower_id_str}) " # keep this merge separate from below o/w multiple nodes can be created
		"MERGE (user) <-[follows_rel:FOLLOWS]- (follower) "
		"ON CREATE SET follows_rel.from = {now}, follows_rel.to = {now} "
		"ON MATCH SET follows_rel.to = {now}",
		{"screen_name":screen_name, "follower_ids_str":follower_ids_str, "now":timestamp})
	# Now, for all FOLLOWS whose "to" field is not current timestamp, make them FOLLOWED
	session.run(
		"MATCH (user:USER {screen_name:{screen_name}}) <-[follows_rel:FOLLOWS]- (follower:USER) "
		"WHERE follows_rel.to <> {now} "
		"CREATE (user) <-[:FOLLOWED {from:follows_rel.from, to:follows_rel.to}]- (follower) "
		"DELETE follows_rel",
		{"screen_name":screen_name, "now":timestamp})

timestamp = datetime.now().timestamp()
create_user(1,"1","Abhishek",{"m1":"d1","m2":"d2"},timestamp)
add_user_info_to_linked_list("Abhishek",{"m1":"d3","m2":"d4"},timestamp+1)
add_user_info_to_linked_list("Abhishek",{"m1":"d5","m2":"d6"},timestamp+2)
update_followers("Abhishek", ["f1","f2"], timestamp+3)
update_followers("Abhishek", ["f2","f3"], timestamp+4)
update_followers("Abhishek", ["f1","f2"], timestamp+5)
update_followers("Abhishek", [], timestamp+6)
update_followers("Abhishek", ["f1","f4"], timestamp+7)
session.close()