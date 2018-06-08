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

sys.path.append("/home/db1/Documents/TwitterAnalytics/dashboard_website/myapp/mongo")
from ingest_raw import MongoQuery

queries = \
{'active_users': ['\n'
                  'UNWIND {um_id} AS um_id_value\n'
                  'UNWIND {hashtag} AS hashtag_value\n'
                  'MATCH (user :USER) -[:TWEETED]-> (t1 :TWEET), (user ) -[:TWEETED]-> (t2 :TWEET), (user ) -[:TWEETED]-> (t3 :TWEET), (t1 ) -[:HAS_HASHTAG]-> (:HASHTAG {text:hashtag_value}), (t2 ) '
                  '-[:HAS_MENTION]-> (user_mentioned :USER {id:um_id_value})\n'
                  'RETURN user.id as userid, count(distinct t3) as count\n',
                  ['hashtag', 'um_id'],
                  ['count', 'userid']],
 'most_popular_hashtags_20': ['mp_ht_in_total', [], ['count', 'hashtag']],
 'most_popular_mentions_20': ['mp_um_in_total', [], ['count', 'userId']]}
types = \
{'active_users': 'neo4j', 'most_popular_hashtags_20': 'mongoDB', 'most_popular_mentions_20': 'mongoDB'}
provided_inputs = \
{'n1': {'limit': '20'}, 'n2': {'limit': '20'}, 'n3': {}}
mapping = \
{'n1': {}, 'n2': {}, 'n3': {'hashtag': ('n1', 'hashtag'), 'um_id': ('n2', 'userId')}}
node_to_query = \
{'n1': 'most_popular_hashtags_20', 'n2': 'most_popular_mentions_20', 'n3': 'active_users'}


def get_task_from_node(node):
	return "node_"+node

args = {
	'owner': 'airflow',
	'start_date': datetime.now(),
}

dag = DAG(dag_id='active_users_dag', default_args=args,schedule_interval=None)

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
			inp = context['task_instance'].xcom_pull(task_ids=get_task_from_node(mapp[0]),dag_id = "active_users_dag",key=mapp[1])
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
