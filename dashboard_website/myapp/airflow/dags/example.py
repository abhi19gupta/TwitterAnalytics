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

from airflow.hooks import DbApiHook
args = {
	'owner': 'airflow',
	'start_date': datetime.now(),
}

dag = DAG(dag_id='example', default_args=args,schedule_interval=None)

# def execute_query(query_name,inputs):
# 	mongoQuery = MongoQuery()
# 	# hook = DbApiHook('../../../db.sqlite3')
# 	# conn = hook.get_conn()
# 	# conn = sqlite3.connect('../../../db.sqlite3')
# 	# q = Query.objects.get(name=query_name)
# 	# c = conn.cursor()
# 	# c.execute("SELECT name FROM sqlite_master WHERE type='table';")
# 	# c.execute("SELECT * FROM myapp_query")
# 	# print(c.fetchall())
# 	# c.execute('SELECT * FROM myapp_query WHERE name=?',(query_name,))
# 	# q = c.fetchone()
# 	q = (13, 'q7', 'MATCH(n) RETURN COUNT(n)', 'neo4j')
# 	print(q)
# 	query_id = q[0]

# 	# outputs = [x.output_name for x in QueryOutput.objects.filter(query=q)]
# 	# c.execute('SELECT * FROM myapp_queryoutput WHERE query_id=?',(query_id,))
# 	# outputs = [x[1] for x in c.fetchall()] 
# 	outputs = ["id_","counts"]
# 	print(outputs)
# 	ret = {out:[] for out in outputs}
# 	# conn.commit()
# 	# conn.close()
# 	print("========================================")
# 	print("Executing query ",query_name)
# 	pprint(inputs)
# 	if(q[3]=='neo4j'):
# 		driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
# 		session = driver.session()
# 		result = session.run(q[2],inputs)
# 		ret = {x:[] for x in outputs}
# 		try:
# 			for record in result:
# 				print(record)
# 				for out in outputs:
# 					if(isinstance(record[out],bytes)):
# 						ret[out].append(record[out].decode("utf-8"))
# 					else:
# 						ret[out].append(record[out])
# 		except:
# 			pass
# 	if(q[3]=="mongoDB"):
# 		if(q[2]=="mp_ht_in_total"):
# 			print(inputs["num"])
# 			ret = mongoQuery.mp_ht_in_total(limit=inputs["num"])
# 	print(ret)
# 	print("========================================")
# 	return ret

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
	if(q[3]=='neo4j'):
		driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
		session = driver.session()
		result = session.run(q[2],inputs)
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
	if(q[3]=="mongoDB"):
		if(q[2]=="mp_ht_in_total"):
			print(inputs["num"])
			ret = mongoQuery.mp_ht_in_total(limit=inputs["num"])
	print(ret)
	print("========================================")
	return ret


task = PythonOperator(
		task_id='query_{}'.format("q6"),
		python_callable=execute_query,
		op_kwargs={'query_name':"q7","inputs":{}},
		dag=dag)

# execute_query("q6",{"num":5})