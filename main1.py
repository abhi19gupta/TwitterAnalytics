# TO CLEAR THE DATABSE:
# MATCH (a)-[r]->(b) DELETE r
# MATCH (a) DELETE a

from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
session = driver.session()

def add_user_info_to_linked_list(screen_name, user_info_dict):
	session.run(
		# Don't do the following, it will match nothing if there is no current state
		# "MATCH (user:User {screen_name:{screen_name}}) -[curr_state:CURR_STATE]-> (prev_user_info:USER_INFO) "
		"MATCH (user:USER {screen_name:{screen_name}})"
		"CREATE (user) -[:CURR_STATE]-> (curr_user_info:USER_INFO {user_info_dict}) "
		"WITH user, curr_user_info "
		"MATCH (curr_user_info) <-[:CURR_STATE]- (user) -[prev_state:CURR_STATE]-> (prev_user_info) "
		"DELETE prev_state "
		"CREATE (curr_user_info) -[:PREV]-> (prev_user_info)",
		{"screen_name":screen_name, "user_info_dict":user_info_dict})

def create_node(id,id_str,screen_name,metadata):
	results = session.run(
		"CREATE (user:USER {id: {id}, id_str: {id_str}, screen_name: {screen_name}}) -[:CURR_STATE]-> (state:USER_INFO {metadata}), "
		"(user) -[:INITIAL_STATE]-> (state) "
		"RETURN user,state",
		{"id":id, "id_str":id_str, "screen_name":screen_name, "metadata":metadata})
	for result in results:
		print(result["user"])

create_node(1,"1","Abhishek",{"m1":"d1","m2":"d2"})
add_user_info_to_linked_list("Abhishek",{"m1":"d3","m2":"d4"})
add_user_info_to_linked_list("Abhishek",{"m1":"d5","m2":"d6"})
session.close()