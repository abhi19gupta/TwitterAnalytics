from neo4j.v1 import GraphDatabase, basic_auth, types

l = ["MPN","bookmeter","GOT7"]
# q = """
# MATCH (n:USER)-[:TWEETED]->(t:TWEET)-[:HAS_HASHTAG]->(h:HASHTAG)
# RETURN n.id,h.text
# """
# q = """
# MATCH (h:HASHTAG)
# RETURN h.text
# """
q = """
match (u :USER)-[:TWEETED]->(t :TWEET)-[:HAS_HASHTAG]->(h :HASHTAG {text:"bookmeter"}) return count(u)
"""
# q="""
# MATCH (u :USER) -[:TWEETED]-> (t :TWEET)
# RETURN u.id,count(t)
# """
q="""
UNWIND {hash} AS hash_value
MATCH (u :USER) -[:TWEETED]-> (t :TWEET), (t) -[:HAS_HASHTAG]-> (:HASHTAG {text:hash_value})
WITH DISTINCT u
MATCH (u) -[:TWEETED]-> (t1 :TWEET)
RETURN u.id,count(t1)
"""
# q = """
# match(t :TWEET) return count(t)
# """
driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
session = driver.session()
result = session.run(q,{"hash":l})
# result = session.run(q)

outputs = ["u.id","count(t1)"]
# for record in result:
# 	print(record)
try:
	d = {x:[] for x in outputs}
	for record in result:
		print(record)
		for out in outputs:
			if(isinstance(record[out],bytes)):
				d[out].append(record[out].decode("utf-8"))
			else:
				d[out].append(record[out])
except:
	pass
print(d)

