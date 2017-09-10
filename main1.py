# TO CLEAR THE DATABSE:
# MATCH (a)-[r]->(b) DELETE r
# MATCH (a) DELETE a

from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
session = driver.session()

INFI_TIME = 1000000000000

def create_user(id,id_str,screen_name,user_info_dict,timestamp):
	results = session.run(
		"CREATE (user:USER {id: {id}, id_str: {id_str}, screen_name: {screen_name}}) "
		"-[:CURR_STATE {from:{now}, to:{infi}}]-> "
		"(state:USER_INFO {user_info_dict}), "
		"(user) -[:INITIAL_STATE {on:{now}}]-> (state) "
		"RETURN user,state",
		{"id":id, "id_str":id_str, "screen_name":screen_name, "user_info_dict":user_info_dict, "now":timestamp, "infi":INFI_TIME})
	for result in results:
		print(result["user"])

def add_user_info_to_linked_list(screen_name,user_info_dict,timestamp):
	session.run(
		# Don't do the following, it will match nothing if there is no current state
		# "MATCH (user:User {screen_name:{screen_name}}) -[curr_state:CURR_STATE]-> (prev_user_info:USER_INFO) "
		"MATCH (user:USER {screen_name:{screen_name}})"
		"CREATE (user) -[:CURR_STATE {from:{now}, to:{infi}}]-> (curr_user_info:USER_INFO {user_info_dict}) "
		"WITH user, curr_user_info "
		"MATCH (curr_user_info) <-[:CURR_STATE]- (user) -[prev_state_rel:CURR_STATE]-> (prev_user_info) "
		"CREATE (curr_user_info) -[:PREV {from:prev_state_rel.from, to:{now}}]-> (prev_user_info)"
		"DELETE prev_state_rel ",
		{"screen_name":screen_name, "user_info_dict":user_info_dict, "now":timestamp, "infi":INFI_TIME})

def add_followers(screen_name, follower_ids):
	session.run(
		"MATCH (user:USER {screen_name:{screen_name}) "
		"UNWIND {follwer_ids} AS follower_id "
		"MERGE (user) <-[follows_rel:FOLLOWS]- (follower:USER_ {id:follower_id})"
		"ON CREATE SET follows_rel."
		"ON MATCH ")

timestamp = datetime.now().timestamp()
create_user(1,"1","Abhishek",{"m1":"d1","m2":"d2"},timestamp)
add_user_info_to_linked_list("Abhishek",{"m1":"d3","m2":"d4"},timestamp+1)
add_user_info_to_linked_list("Abhishek",{"m1":"d5","m2":"d6"},timestamp+2)
session.close()