from __future__ import print_function
from airflow.operators import PythonOperator
from airflow.models import DAG
from datetime import datetime
import sys
import sqlite3
import copy
import os
from pprint import *
from neo4j.v1 import GraphDatabase, basic_auth, types

sys.path.append("/home/db1/Documents/TwitterAnalytics/Dashboard_Website/myapp/mongo")
from ingest_raw import MongoQuery

queries = \
{'active_users': ['\n'
                  'UNWIND {users_in} AS users_in_value\n'
                  'UNWIND {hash_in} AS hash_in_value\n'
                  'MATCH (user :USER) -[:TWEETED]-> (t1 :TWEET), (user ) -[:TWEETED]-> (t2 :TWEET), (user ) -[:TWEETED]-> (t3 :TWEET), (t1 ) -[:HAS_HASHTAG]-> (:HASHTAG {text:hash_in_value}), (t2 ) '
                  '-[:HAS_MENTION]-> (user_mentioned :USER {id:users_in_value})\n'
                  'RETURN user.id as userid, count(distinct t3) as tweet_counts\n',
                  ['hash_in', 'users_in'],
                  ['tweet_counts', 'userid']],
 'got7_users': ['\n'
                '\n'
                'MATCH (run:RUN) -[:HAS_FRAME]-> (frame1:FRAME)\n'
                'WHERE  frame1.end_t >= 1525745939.0 AND frame1.start_t <= 1525874405.0 \n'
                "MATCH (frame1) -[:HAS_TWEET]-> (event1 :TWEET_EVENT), (event1) -[:TE_USER]-> (u :USER), (event1) -[:TE_TWEET]-> (t :TWEET), (t ) -[:HAS_HASHTAG]-> (:HASHTAG {text:'GOT7'})\n"
                'RETURN u,count(t)\n',
                [],
                ['u', 'count(t)']],
 'most_popular_hashtags_3': ['mp_ht_in_total', [], ['count', 'hashtag']],
 'most_popular_mentions_3': ['mp_um_in_total', [], ['count', 'userId']],
 'plot_user_count': ['def func(inputs):\n'
                     '\tinputs = list(zip(inputs["userid"], inputs["count"]))\n'
                     '\tinputs.sort(key=lambda item:item[1], reverse=True)\n'
                     '\tx_vals = []\n'
                     '\ty_vals = []\n'
                     '\tfor i in range(10):\n'
                     '\t\tx_vals.append(str(inputs[i][0]))\n'
                     '\t\ty_vals.append(inputs[i][1])\n'
                     '\n'
                     '\tret = {}\n'
                     '\tret["x_vals"] = x_vals\n'
                     '\tret["y_vals"] = y_vals\n'
                     '\treturn ret',
                     ['userid', 'count'],
                     ['x_vals', 'y_vals']]}
types = \
{'active_users': 'neo4j', 'got7_users': 'neo4j', 'most_popular_hashtags_3': 'mongoDB', 'most_popular_mentions_3': 'mongoDB', 'plot_user_count': 'postProcessing'}
provided_inputs = \
{'n1': {'limit': '3'}, 'n2': {'limit': '3'}, 'n3': {}}
mapping = \
{'n1': {}, 'n2': {}, 'n3': {'hash_in': ('n1', 'hashtag'), 'users_in': ('n2', 'userId')}}
node_to_query = \
{'n1': 'most_popular_hashtags_3', 'n2': 'most_popular_mentions_3', 'n3': 'active_users'}


def get_task_from_node(node):
	return "node_"+node

args = {
	'owner': 'airflow',
	'start_date': datetime.now(),
}

dag = DAG(dag_id='most_active_users', default_args=args,schedule_interval=None)

def execute_query(node_name,**context):
	query_to_node = {v:k for k,v in node_to_query.items()}
	query_name = node_to_query[node_name]
	mongoQuery = MongoQuery()

	query_code  = queries[query_name][0]
	input_vars = queries[query_name][1]
	constant_vars = list(provided_inputs[query_to_node[query_name]].keys())
	output_vars = queries[query_name][2]
	query_type = types[query_name]
	ret = {out:[] for out in output_vars}

	inputs = {}
	for x in set(input_vars+constant_vars):
		if(x not in mapping[node_name] or mapping[node_name][x]=="-"):
			inputs[x] = provided_inputs[node_name][x]
		else:
			mapp = mapping[node_name][x]
			inp = context['task_instance'].xcom_pull(task_ids=get_task_from_node(mapp[0]),dag_id = "most_active_users",key=mapp[1])
			print(inp)
			inputs[x] = inp
	print("========================================")
	print("Executing query ",query_name)
	pprint(inputs)
	if(query_type=='neo4j'):
		driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
		session = driver.session()
		result = session.run(query_code,inputs)
		# ret = {x:[] for x in outputs}
		try:
			for record in result:
				print(record)
				for out in output_vars:
					if(isinstance(record[out],bytes)):
						ret[out].append(record[out].decode("utf-8"))
					else:
						ret[out].append(record[out])
		except:
			print("Came into except ")
	elif(query_type=="mongoDB"):
		temp = inputs
		if(query_code=="mp_ht_in_total"):
			ret = mongoQuery.mp_ht_in_total(**temp)
		elif(query_code=="mp_ht_in_interval"):
			ret = mongoQuery.mp_ht_in_interval(**temp)
		elif(query_code=="ht_in_interval"):
			ret = mongoQuery.ht_in_interval(**temp)
		elif(query_code=="ht_with_sentiment"):
			ret = mongoQuery.ht_with_sentiment(**temp)
		elif(query_code=="mp_um_in_total"):
			ret = mongoQuery.mp_um_in_total(**temp)

	elif(query_type=="postProcessing"):
		context = {"inputs":copy.deepcopy(inputs)}
		try:
			compile(q.query,'','exec')
			exec(q.query + "\n" + "ret = func(inputs)", context)
			for out in outputs:
				ret[out] = context[out]
		except Exception as e:
			print("Exeption while executing Post proc function: %s, %s"%(type(e),e))

	print(ret)
	for k,v in ret.items():
		context['task_instance'].xcom_push(k,v)
	print("========================================")
	return ret

task_0 = PythonOperator(
		task_id='node_{}'.format("n1"),
		python_callable=execute_query,
		op_kwargs={'node_name':"n1"},
		provide_context = True,
		dag=dag)

task_1 = PythonOperator(
		task_id='node_{}'.format("n2"),
		python_callable=execute_query,
		op_kwargs={'node_name':"n2"},
		provide_context = True,
		dag=dag)

task_2 = PythonOperator(
		task_id='node_{}'.format("n3"),
		python_callable=execute_query,
		op_kwargs={'node_name':"n3"},
		provide_context = True,
		dag=dag)
task_0 >> task_2
task_1 >> task_2
