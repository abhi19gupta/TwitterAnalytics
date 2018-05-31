from __future__ import print_function
from airflow.operators import PythonOperator
from airflow.models import DAG
from datetime import datetime
import sys
import sqlite3
from pprint import *
from neo4j.v1 import GraphDatabase, basic_auth, types

sys.path.append("/home/db1/Documents/TwitterAnalytics/dashboard_website/myapp")
from ingest_raw import MongoQuery

queries = \
{'q10': ['MATCH(n) RETURN COUNT(n)', [], []],
 'q3': ['\nMATCH (u :USER) -[:TWEETED]-> (t :TWEET)\nRETURN u,count(t)\n', [], []],
 'q4': ['UNWIND {hash} AS hash_value\r\n'
        'MATCH (u :USER) -[:TWEETED]-> (t :TWEET), (t) -[:HAS_HASHTAG]-> (:HASHTAG {text:hash_value})\r\n'
        'WITH DISTINCT u\r\n'
        'MATCH (u) -[:TWEETED]-> (t1 :TWEET)\r\n'
        'RETURN u.id,count(t1)',
        ['hash'],
        ['count(t1)', 'u.id']],
 'q6': ['mp_ht_in_total', ['num'], ['count', '_id']],
 'q7': ['UNWIND {xid} as id_value\r\nMATCH (u :USER {id:id_value})-[:TWEETED]->(t :TWEET)-[:HAS_HASHTAG]->(h :HASHTAG)\r\nWITH DISTINCT h\r\nRETURN h.text', ['xid'], ['h.text']]}
types = \
{'q10': 'neo4j', 'q3': 'neo4j', 'q4': 'neo4j', 'q6': 'mongoDB', 'q7': 'neo4j'}
provided_inputs = \
{'n1': {'num': 5}, 'n2': {}}
mapping = \
{'n1': {'num': '-'}, 'n2': {'hash': ('n1', '_id')}}
node_to_query = \
{'n1': 'q6', 'n2': 'q4'}


def get_task_from_node(node):
	return "node_"+node

args = {
	'owner': 'airflow',
	'start_date': datetime.now(),
}

dag = DAG(dag_id='fourth', default_args=args,schedule_interval=None)

def execute_query(node_name,**context):
	query_name = node_to_query[node_name]
	mongoQuery = MongoQuery()

	query_code  = queries[query_name][0]
	input_vars = queries[query_name][1]
	output_vars = queries[query_name][2]
	query_type = types[query_name]
	ret = {out:[] for out in output_vars}

	inputs = {}
	for x in input_vars:
		if(mapping[node_name][x]=="-"):
			inputs[x] = provided_inputs[node_name][x]
		else:
			mapp = mapping[node_name][x]
			inp = context['task_instance'].xcom_pull(task_ids=get_task_from_node(mapp[0]),dag_id = "fourth",key=mapp[1])
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
	if(query_type=="mongoDB"):
		if(query_code=="mp_ht_in_total"):
			print(inputs["num"])
			ret = mongoQuery.mp_ht_in_total(limit=inputs["num"])
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
task_0 >> task_1
