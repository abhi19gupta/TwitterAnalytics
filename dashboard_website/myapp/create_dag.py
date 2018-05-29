from __future__ import print_function
import os, time
import numpy as np
import networkx as nx
from pprint import *
import jinja2 as jj
import os
##expect a dictionary of queries with keys as the query names/postprocessing function names
## for neo4j queries it will be the query code, input, output list
## for mongoDB queries it will be the partially formatted query specification, input, output list
## for post processing functions it will be the function_definition, input, output list
# queries = {"q1":[],"q2":[],...}
# types = {"q1":"neo4j","q2":"mongoDB","q3":"PostProcesing",....}

#Expect that there is a function to evaluate a query given its inputs(as a dictionary) and returns a dictionary of outputs

imports = """from __future__ import print_function
from airflow.operators import PythonOperator
from airflow.models import DAG
from datetime import datetime
import sys
import sqlite3
from pprint import *
from neo4j.v1 import GraphDatabase, basic_auth, types

sys.path.append("/home/db1/Documents/TwitterAnalytics/dashboard_website/myapp")
from ingest_raw import MongoQuery
"""
## task is (number, associated_query, inputs(a string whose eval will give you a dictionay))
task_template = """
task_{{t[0]}} = PythonOperator(
		task_id='node_{}'.format("{{t[1]}}"),
		python_callable=execute_query,
		op_kwargs={'node_name':"{{t[1]}}"},
		provide_context = True,
		dag=dag)
"""

queries = """queries = {{queries}}"""
types = """types = {{types}}"""
provided_inputs = "provided_inputs = {{provided_inputs}}"

dag_defn = """args = {
	'owner': 'airflow',
	'start_date': datetime.now(),
}

dag = DAG(dag_id='{{dag_name}}', default_args=args,schedule_interval=None)
"""

helper_fns = """
def get_task_from_node(node):
	return "node_"+node
"""

execute_fn = """
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
			inp = context['task_instance'].xcom_pull(task_ids=get_task_from_node(mapp[0]),dag_id = "{{dag_name}}",key=mapp[1])
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
"""

class DAG:
	def __init__(self,network_file,queries,types):
		self.queries = queries
		self.types = types
		print("the queries and types are ")
		pprint(self.queries)
		pprint(self.types)
		self.outputs_dict = {}
		for q in queries.keys():
			self.outputs_dict[q] ={}
			for out in queries[q][2]:
				self.outputs_dict[q][out] = ""
		self.graph = nx.DiGraph()
		self.node_to_query = {}
		self.returns = []
		self.inputs = {}
		self.connections = {}
		# self.taking_inputs = {}
		# self.giving_outputs = []
		# fin = open(network_file,"r")
		fin = network_file
		s = fin.read()
		if(isinstance(s,bytes)):
			s = s.decode()
		cond = -1
		count = 0
		for line in s.split("\n"):
			count+=1
			print(line)
			if(line.strip()==""):
				continue
			if(count==1):
				l = line.strip().split()
				self.num_nodes = int(l[0])
				# self.num_edges = int(l[1])
			elif(count<=1+self.num_nodes):
				node_name, query_name = line.strip().split()
				self.graph.add_node(node_name,type=self.types[query_name],
					query_name = query_name,query = self.queries[query_name][0])
				self.node_to_query[node_name] = query_name
				self.inputs[node_name]  ={}
				self.connections[node_name]  ={}

			else:
				if(line.strip()=="CONNECTIONS:"):
					cond = 1
					continue
				elif(line.strip()=="INPUTS:"):
					cond = 0
					continue
				elif(line.strip()=="RETURNS:"):
					cond = 2
					continue
				if(cond==1):
					l = line.strip().split()
					ind1 = l[0].index(".")
					ind2 = l[1].index(".")
					u,outp = l[0][:ind1],l[0][ind1+1:]
					v,inp = l[1][:ind2],l[1][ind2+1:]
					print(u,outp,v,inp)
					self.graph.add_edge(u,v,mapping=(outp,inp))
					self.connections[v][inp] = (u,outp)
				elif(cond==0):
					l = line.strip().split()
					ind = l[0].index(".")
					node_name,var_name = l[0][:ind],l[0][ind+1:]
					# self.taking_inputs[self.graph.node[node_name]["query_name"]+"."+var_name] = eval(l[1])
					self.inputs[node_name][var_name] = eval(l[1])
					self.connections[node_name][var_name] = "-"
				elif(cond==2):
					s = line.strip()
					ind = s.index(".")
					node_name,var_name = s[:ind], s[ind+1:]
					# self.giving_outputs.append(self.graph.node[node_name]["query_name"]+"."+var_name)
					self.returns.append((node_name,var_name))

		# for q in self.inputs.keys():
		# 	for k,v in self.inputs[q]:
		# 		self.taking_inputs[q+"."+k] = v

		is_dag = nx.is_directed_acyclic_graph(self.graph)
		if(not(is_dag)):
			print("The input file doesn't specify a DAG. Please Check!!")

		print("-----------")
	
	def generate_dag(self,dag_name):
		print(os.getcwd())
		fout = open("myapp/airflow/dags/"+dag_name+".py","w")

		print(imports,file=fout)

		print("queries = \\",file=fout)
		pprint(self.queries,fout,width=200)

		print("types = \\",file=fout)
		pprint(self.types,fout,width=200)

		print("provided_inputs = \\",file=fout)
		pprint(self.inputs,fout,width=400)

		print("mapping = \\",file=fout)
		pprint(self.connections,fout,width=400)

		print("node_to_query = \\",file=fout)
		pprint(self.node_to_query,fout,width=400)

		print("",file=fout)
		print(helper_fns,file=fout)

		dd  =jj.Template(dag_defn)
		dd_c = dd.render(dag_name=dag_name)
		print(dd_c,file=fout)

		ef  =jj.Template(execute_fn)
		ef_c = ef.render(dag_name=dag_name)
		print(ef_c,file=fout)

		node_to_task = {}
		for i,node in enumerate(self.node_to_query.keys()):
			tt = jj.Template(task_template)
			t_c = tt.render(t = (i,node))
			node_to_task[node] = "task_"+str(i)
			print(t_c,file=fout)

		for u,v in self.graph.edges():
			print(node_to_task[u] + " >> " + node_to_task[v],file=fout)
		fout.close()

if __name__=="__main__":
	queries = {"q1":["query 1",["inp1","inp2"],["out1","out2"]],
			"q2":["query 2",["inp1"],["out1","out2"]],
			"q3":["query 3",["inp1","inp2","inp3"],["out1"]]}
	types = {"q1":"mongoDB","q2":"PostProcesing","q3":"neo4j"}
	dag = DAG(open("input_graph.txt","r"),queries,types)
	dag.generate_dag("my_dag")
