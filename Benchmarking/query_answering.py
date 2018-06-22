"""
Module to benchmark the query answering rate for neo4j.

	* Generate a list of queries.
	* Open multple neo4j sessions.
	* See what is the peak rate.
"""
from __future__ import print_function
from pprint import *
import jinja2 as jj
import random, time
from neo4j.v1 import GraphDatabase, basic_auth
from multiprocessing import Pool

q1 = """match (u:USER)-[:TWEETED]->(t:TWEET)-[:HAS_HASHTAG]->(:HASHTAG {text:"{{h}}"}) with distinct u as u1 return count(u1)"""
q2 = """match (x:USER)-[:FOLLOWS]->(:USER {id:{{u1}}}), (x)-[:FOLLOWS]->(:USER {id:{{u2}}}) with distinct x as x1 return count(x1)"""
q3 = """
match (fe:FOLLOW_EVENT)-[:FE_FOLLOWED]->(u:USER {id:{{u}}})
where fe.timestamp > {{t1}} and fe.timestamp < {{t2}}
return count(fe)
"""
q4 = """
match (x:USER {id:{{u}}})-[:TWEETED]->(:TWEET)-[:HAS_HASHTAG]->(h:HASHTAG), (f:USER)-[:FOLLOWS]->(x), (f)-[:TWEETED]->(:TWEET)-[:HAS_HASHTAG]->(h)
with distinct f as f1
return count(f1)
"""
q5 = """
match (te:TWEET_EVENT)-[:TE_TWEET]->(:TWEET)-[:RETWEET_OF]->(t:TWEET), (te)-[:TE_USER]->(:USER {id:{{u}}}), (x:USER)-[:TWEETED]->(t)
where te.timestamp < {{t1}} and te.timestamp > {{t2}}
with distinct x as x1
return count(x1)
"""
#####################

##find common followers of id1 and id2
q6 = """
MATCH (u1 :USER {id:{{id1}}}),(u2 :USER {id:{{id2}}}), (user :USER)
WHERE (u1) <-[:FOLLOWS]- (user) AND (user) -[:FOLLOWS]-> (u2)
RETURN count(user)
"""

##find the users which follow a user u and tweeted t(which mentions same u) b/w t1 and t2
q7="""
MATCH (run:RUN) -[:HAS_FRAME]-> (frame1:FRAME)
WHERE  frame1.end_t >= {{t1}} AND frame1.start_t <= {{t2}}
MATCH (frame1) -[:HAS_TWEET]-> (event1 :TWEET_EVENT), (event1) -[:TE_USER]-> (x :USER {}), (event1) -[:TE_TWEET]-> (t :TWEET {}), (x :USER {}) -[:FOLLOWS]-> (u :USER {id:{{u}}}), (t) -[:HAS_MENTION]-> (u :USER {id:{{u}}})
RETURN  COUNT(x)
"""
##find users which have tweeted t1(retweet of t containing hash AND t1 itself contains hash) b/w t1 and t2 and  follows u
q8 = """
MATCH (run:RUN) -[:HAS_FRAME]-> (frame1:FRAME)
WHERE  frame1.end_t >= {{t1}} AND frame1.start_t <= {{t2}}
MATCH (frame1) -[:HAS_TWEET]-> (event1 :TWEET_EVENT), (event1) -[:TE_USER]-> (x :USER {}), (event1) -[:TE_TWEET]-> (t1 :TWEET {}), (x :USER {}) -[:FOLLOWS]-> (u :USER {id:{{u}}}), (t :TWEET {}) -[:HAS_HASHTAG]-> (:HASHTAG {text:'{{hash}}'}), (t1 :TWEET {}) -[:HAS_HASHTAG]-> (:HASHTAG {text:'{{hash}}'}), (t1) -[:RETWEET_OF]-> (t)
RETURN  COUNT(x)
"""

##find users which follows u1 with id u1 and follow u which tweeted b/w t1 and t2 containing hashtag hash
q9 = """
MATCH (run:RUN) -[:HAS_FRAME]-> (frame1:FRAME)
WHERE  frame1.end_t >= {{t1}} AND frame1.start_t <= {{t2}}
MATCH (frame1) -[:HAS_TWEET]-> (event1 :TWEET_EVENT), (event1) -[:TE_USER]-> (u :USER {}), (event1) -[:TE_TWEET]-> (t :TWEET {}), (x :USER {}) -[:FOLLOWS]-> (u1 :USER {id:{{u1}}}), (x) -[:FOLLOWS]-> (u), (t :TWEET {}) -[:HAS_HASHTAG]-> (:HASHTAG {text:'{{hash}}'})
RETURN  COUNT(x)
"""
if __name__=="__main__":
	driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
queries = [q1,q2,q3,q4,q5]
hashtags = [x["_id"] for x in [{'_id': 'MPN', 'count': 6199}, {'_id': 'bookmeter', 'count': 3242}, {'_id': 'SorryNotSorryClipe', 'count': 3167}, {'_id': 'izmirescort', 'count': 3157}, {'_id': 'EduChociayRevelacao', 'count': 2944}, {'_id': 'トレクル', 'count': 1991}, {'_id': 'bucaescort', 'count': 1005}, {'_id': 'GOT7', 'count': 911}, {'_id': 'Mersal', 'count': 891}, {'_id': 'YouAre', 'count': 697}, {'_id': '워너원', 'count': 694}, {'_id': '방탄소년단', 'count': 693}, {'_id': 'BTS', 'count': 635}, {'_id': 'OErradoSouEuClipe', 'count': 617}, {'_id': '갓세븐', 'count': 573}, {'_id': '김재환', 'count': 560}, {'_id': '平スト', 'count': 548}, {'_id': 'รูปที่เซฟไว้ดูตอนเครียด', 'count': 468}, {'_id': '7for7', 'count': 445}, {'_id': 'baystars', 'count': 442}, {'_id': 'الرياض', 'count': 428}, {'_id': '뷔', 'count': 425}, {'_id': 'EXO', 'count': 421}, {'_id': '태형', 'count': 389}, {'_id': 'NadineLustre', 'count': 376}, {'_id': 'KCAPinoyStar', 'count': 347}, {'_id': 'taekook', 'count': 339}, {'_id': 'V', 'count': 332}, {'_id': 'BTSBBMAs', 'count': 328}, {'_id': 'WANNAONE', 'count': 327}, {'_id': 'BamBam', 'count': 301}, {'_id': 'BigDickBigShot', 'count': 299}, {'_id': '너블들의_퀘스찬', 'count': 292}, {'_id': 'MersalTeaser', 'count': 288}, {'_id': '변명말고_팩트를', 'count': 281}, {'_id': 'EquiparacionYa', 'count': 257}, {'_id': 'ALDUBBigHearts', 'count': 256}, {'_id': 'BBBH', 'count': 254}, {'_id': 'ALDUBLoveNoEnd', 'count': 250}, {'_id': 'art', 'count': 249}, {'_id': 'bornovaescort', 'count': 248}, {'_id': 'sketchbook', 'count': 247}, {'_id': '정국', 'count': 246}, {'_id': 'AmReading', 'count': 237}, {'_id': '어쩔_수_없지_뭐', 'count': 236}, {'_id': '재환', 'count': 235}, {'_id': '마크', 'count': 234}, {'_id': 'TheCookingGene', 'count': 229}, {'_id': 'ALDUB27thMonthsary', 'count': 225}, {'_id': 'Etsy', 'count': 223}]]
users = [x["_id"] for x in [{'_id': '335141638', 'count': 35073}, {'_id': '1343977518', 'count': 23092}, {'_id': '10228272', 'count': 13748}, {'_id': '791145648171126784', 'count': 13383}, {'_id': '25073877', 'count': 12203}, {'_id': '228395466', 'count': 9220}, {'_id': '237915732', 'count': 8384}, {'_id': '883135139428970497', 'count': 8292}, {'_id': '81071710', 'count': 7488}, {'_id': '2156299840', 'count': 6680}, {'_id': '12371162', 'count': 6610}, {'_id': '47839975', 'count': 6576}, {'_id': '339620073', 'count': 6556}, {'_id': '147810583', 'count': 6494}, {'_id': '873036966933045248', 'count': 6210}, {'_id': '198049087', 'count': 5388}, {'_id': '2156278138', 'count': 5160}, {'_id': '712208574', 'count': 4960}, {'_id': '11522502', 'count': 4862}, {'_id': '2604620971', 'count': 4680}, {'_id': '4795483642', 'count': 4189}, {'_id': '3411018190', 'count': 4110}, {'_id': '2921740543', 'count': 4020}, {'_id': '125650542', 'count': 4008}, {'_id': '725771668824666114', 'count': 3876}, {'_id': '356450858', 'count': 3864}, {'_id': '4675489896', 'count': 3798}, {'_id': '3986785042', 'count': 3792}, {'_id': '165699676', 'count': 3775}, {'_id': '62917505', 'count': 3759}, {'_id': '138476432', 'count': 3726}, {'_id': '215559242', 'count': 3666}, {'_id': '834710312188248064', 'count': 3656}, {'_id': '62195257', 'count': 3648}, {'_id': '1409798257', 'count': 3510}, {'_id': '3221502973', 'count': 3484}, {'_id': '19856378', 'count': 3330}, {'_id': '802415951492747264', 'count': 3300}, {'_id': '811236210693681152', 'count': 3290}, {'_id': '2314740763', 'count': 3227}, {'_id': '3131350487', 'count': 3224}, {'_id': '864485534952964096', 'count': 3210}, {'_id': '2587013887', 'count': 3168}, {'_id': '902381759953645568', 'count': 3102}, {'_id': '836750968108675076', 'count': 3090}, {'_id': '23791197', 'count': 3088}, {'_id': '47491330', 'count': 3067}, {'_id': '93164772', 'count': 3040}, {'_id': '271546864', 'count': 3018}, {'_id': '71617621', 'count': 2980}]]

t1,t2 = "1500486521","1501496521"

def generate_random_queries(total):
	"""
	Generate a list containing <total> cypher queries. Consider templated cypher queries and lists of attributes. Randomly put in the attributs into the cypher
	query tempate to get an executable query.

	:param total: number of queris to generate

	.. note:: Obiviously, one need to change the list of attibutes accordingly, if they choose to benchmark on a different dataset.
	"""
	fout = open("queries.txt","w")
	lis = []
	for i in range(total):
		# q_template = jj.Template(q1)
		# q1_code = q_template.render(h=random.choice(hashtags))

		# q_template = jj.Template(q2)
		# q2_code = q_template.render(u1=random.choice(users),u2=random.choice(users))

		# q_template = jj.Template(q3)
		# q3_code = q_template.render(u=random.choice(users),t1=t1,t2=t2)

		# q_template = jj.Template(q4)
		# q4_code = q_template.render(u=random.choice(users))

		# q_template = jj.Template(q5)
		# q5_code = q_template.render(u=random.choice(users),t1=t1,t2=t2)

		q_template = jj.Template(q6)
		q6_code = q_template.render(id1 = random.choice(users), id2 = random.choice(users))

		# q_template = jj.Template(q7)
		# q7_code = q_template.render(t1=t1,t2=t2,u=random.choice(users))

		q_template = jj.Template(q8)
		q8_code = q_template.render(t1=t1,t2=t2,u=random.choice(users),hash = random.choice(hashtags))

		# q_template = jj.Template(q9)
		# q9_code = q_template.render(t1=t1,t2=t2,u1=random.choice(users),hash = random.choice(hashtags))
		lis += [q6_code,q8_code]
	pprint(lis,fout)
	fout.close()
	return lis

def answer_query(query):
	"""
	Answer a single cypher query <query>

	:param query: cypher query to be answered
	"""
	with driver.session() as session:
		result = session.run(query,{})
		for r in result:
			return r

def answer_queries(query_l):
	"""
	Function to answer all queries from a list of queries in sequential manner

	:param query_l: the list of cypher queries
	"""
	print("Ansering the queries sequentially...")
	t1 = time.time()
	ans = []
	for q in query_l:
		ans.append(answer_query(q))
	time_taken = time.time()-t1
	print("Done in time ",time_taken)
	return(ans,time_taken)

def answer_queries_par(query_l,num_procs):
	"""
	Function to answer all queries from a list of queries in concurrent manner. Spawn <num_procs> number
	of processes. Each process opens a session and executes a cypher query.

	:param query_l: the list of cypher queries
	:param num_procs: number of processes to create

	.. note:: There will only be atmost k number of real parallel process in the system, where k is number of cores. This
		number is further limited by the session management of neo4j, which is what we observe in the profile difference between simple and complex queries.
	"""
	print("Ansering the queries with",num_procs,"...")
	t1 = time.time()
	p = Pool(num_procs)
	ans = p.map(answer_query, query_l)
	time_taken = time.time()-t1
	print("Done in time ",time_taken)
	return(ans,time_taken)

if __name__=="__main__":
	# lis = generate_random_queries(1000)
	lis = eval(open("queries.txt","r").read())
	print("Total number of queries: ",len(lis))
	# pprint(lis[:5])
	# ans,t = answer_queries(lis)
	ans,t = answer_queries_par(lis,20)
	print(len(lis)/t)

	# results
	"""
	q1 0.0012
	q2 0.0028
	q3 0.0013
	q4 0.0013
	q5 0.002
	q6 0.165
	q7 0.0022
	q8 0.0396
	q9 0.006
	"""
	"""
	num_procs = [1,2,3,4,5,6,7,8,9,10]
	rate = [585.33, 852.04, 1016.82, 1093.76, 993.98, 981.92, 983.46, 969.79, 949.89, 941.74]
	num_procs = [1,2,3,4,5,6,7,8,10,12,14]
	rate = [12.7, 23.48, 36.37, 47.46, 53.3, 54.67, 61.15, 63.34, 65.45, 59.34, 56.12]
	"""
