
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
{'q1': ['query 1', ['inp1', 'inp2'], ['out1', 'out2']], 'q2': ['query 2', ['inp1'], ['out1', 'out2']], 'q3': ['query 3', ['inp1', 'inp2', 'inp3'], ['out1']]}
types = \
{'q1': 'mongoDB', 'q2': 'PostProcesing', 'q3': 'neo4j'}
provided_inputs = \
{'n1': {'inp1': ['deepak', 'saini'], 'inp2': [1, 2, 3, 4, 5]}, 'n2': {}, 'n3': {}}
mapping = \
{'n1': {'inp1': '-', 'inp2': '-'}, 'n2': {'inp1': ('n1', 'out1')}, 'n3': {'inp1': ('n1', 'out2'), 'inp2': ('n2', 'out1'), 'inp3': ('n2', 'out2')}}
node_to_query = \
{'n1': 'q1', 'n2': 'q2', 'n3': 'q3'}

args = {
	'owner': 'airflow',
	'start_date': datetime.now(),
}

dag = DAG(dag_id='my_dag', default_args=args,schedule_interval=None)

def execute_query(node_name):
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
			inputs[x] = context['task_instance'].xcom_pull(task_ids=mapp[0],key=mapp[1])
	print("========================================")
	print("Executing query ",query_name)
	pprint(inputs)
	if(query_type=='neo4j'):
		driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
		session = driver.session()
		result = session.run(query_code,inputs)
		ret = {x:[] for x in outputs}
		try:
			for record in result:
				print(record)
				for out in outputs:
					if(isinstance(record[out],bytes)):
						ret[out].append(record[out].decode("utf-8"))
					else:
						ret[out].append(record[out])
		except:
			pass
	if(query_type=="mongoDB"):
		if(query_code=="mp_ht_in_total"):
			print(inputs["num"])
			ret = mongoQuery.mp_ht_in_total(limit=inputs["num"])
	print(ret)
	print("========================================")
	return ret


task_0 = PythonOperator(
		task_id='node_{}'.format("n1"),
		python_callable=execute_query,
		op_kwargs={'node_name':"n1"},
		dag=dag)

task_1 = PythonOperator(
		task_id='node_{}'.format("n2"),
		python_callable=execute_query,
		op_kwargs={'node_name':"n2"},
		dag=dag)

task_2 = PythonOperator(
		task_id='node_{}'.format("n3"),
		python_callable=execute_query,
		op_kwargs={'node_name':"n3"},
		dag=dag)
task_0 >> task_1
task_0 >> task_2
task_1 >> task_2
