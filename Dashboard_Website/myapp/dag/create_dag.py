from __future__ import print_function
import os, time
import numpy as np
import networkx as nx
from pprint import *
import jinja2 as jj
import os
import plotly.graph_objs as go
from plotly.graph_objs import *
from plotly.offline import plot
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
import copy
import os
from pprint import *
from neo4j.v1 import GraphDatabase, basic_auth, types

sys.path.append("/home/db1/Documents/TwitterAnalytics/Dashboard_Website/myapp/mongo")
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
			exec(q.query + "\\n" + "ret = func(inputs)", context)
			for out in outputs:
				ret[out] = context[out]
		except Exception as e:
			print("Exeption while executing Post proc function: %s, %s"%(type(e),e))

	print(ret)
	for k,v in ret.items():
		context['task_instance'].xcom_push(k,v)
	print("========================================")
	return ret
"""
# if(query_type=="mongoDB"):
# 		if(query_code=="mp_ht_in_total"):
# 			print(inputs["num"])
# 			ret = mongoQuery.mp_ht_in_total(limit=inputs["num"])
class DAG:
	def __init__(self,network_file_source,queries,types,constants):
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
		self.taking_inputs = {}
		self.giving_outputs = []
		self.edges = []

		s = network_file_source
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
				self.edges = []
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
					self.edges.append((self.graph.node[u]["query_name"]+"."+outp,self.graph.node[v]["query_name"]+"."+inp))
				elif(cond==0):
					l = line.strip().split()
					ind = l[0].index(".")
					node_name,var_name = l[0][:ind],l[0][ind+1:]
					self.taking_inputs[self.graph.node[node_name]["query_name"]+"."+var_name] = eval(l[1])
					self.inputs[node_name][var_name] = eval(l[1])
					self.connections[node_name][var_name] = "-"
				elif(cond==2):
					s = line.strip()
					ind = s.index(".")
					node_name,var_name = s[:ind], s[ind+1:]
					self.giving_outputs.append(self.graph.node[node_name]["query_name"]+"."+var_name)
					self.returns.append((node_name,var_name))

		is_dag = nx.is_directed_acyclic_graph(self.graph)
		if(not(is_dag)):
			print("The input file doesn't specify a DAG. Please Check!!")
		#Treating mongoDB constants as provided inputs in DAG
		self.query_to_node = {v:k for k,v in self.node_to_query.items()}
		for q,v in constants.items():
			# print(q,v)
			for var,val in v.items():
				self.inputs[self.query_to_node[q]][var] = val
		print("-----------")

	def feed_forward(self,execute):
		"""
		Do a topological sort and then do a bfs
		"""
		# outputs_dict = {}
		ts = nx.topological_sort(self.graph)
		for node in ts:
			print(node)
			query_name = self.graph.node[node]["query_name"]
			inputs = self.inputs[node]
			for inp in self.queries[query_name][1]:
				if inp not in inputs.keys():
					print("Node not getting all inputs")
					sys.exit(0)
			outputs = execute(query_name,inputs)
			self.outputs_dict[node] = outputs
			for nbr in self.graph.neighbors(node):
				outp, inp = self.graph.edge[node][nbr]["mapping"]
				self.inputs[nbr][inp] = outputs[outp]
		# rets= {}
		# for ret in self.returns:
		# 	rets[ret] = self.outputs_dict[ret[0]][ret[1]]
		return self.outputs_dict

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

	def get_drawable_dag(self,G,queries,types,edges):
		ts = nx.topological_sort(G)
		dis = nx.DiGraph()
		rectangle = []
		for i,node in enumerate(ts):
			query_name = G.node[node]["query_name"]
			inputs = queries[query_name][1]
			outputs = queries[query_name][2]
			inputs.reverse()
			outputs.reverse()
			length = 2*(max(len(inputs),len(outputs))-1)
			inputs_y = np.linspace(0,length,len(inputs))
			outputs_y = np.linspace(0,length,len(outputs))

			for j,inp in enumerate(inputs):
				dis.add_node(query_name+"."+inp, pos = (i,inputs_y[j]))
			for j,out in enumerate(outputs):
				dis.add_node(query_name+"."+out, pos = (i+0.25,outputs_y[j]))
			rectangle.append((i,length))
		for i,o in edges:
			dis.add_edge(i,o)
		return dis,rectangle


	def plot_dag(self):
		G ,rect= self.get_drawable_dag(self.graph,self.queries,self.types,self.edges)
		print(G.nodes(data=True))
		edge_trace = Scatter(
			x=[],
			y=[],
			line=Line(width=1,color='black'),
			hoverinfo='none',
			mode='arrows')

		for edge in G.edges():
			x0, y0 = G.node[edge[0]]['pos']
			x1, y1 = G.node[edge[1]]['pos']
			edge_trace['x'] += [x0, x1, None]
			edge_trace['y'] += [y0, y1, None]

		node_trace = Scatter(
			x=[],
			y=[],
			text=[],
			mode='markers',
			hoverinfo='text',
			marker=Marker(
				color = [],
				size=10,
				))

		for node in G.nodes():
			x, y = G.node[node]['pos']
			node_trace['x'].append(x)
			node_trace['y'].append(y)
			outs = []
			for q in self.queries.keys():
				for o in self.queries[q][2]:
					outs.append(q+"."+o)
			if(node in outs):
				ind = node.index(".")
				q,val = node[:ind],node[ind+1:]
				node_trace["text"].append(node+"<<"+str(self.outputs_dict[q][val]))
				node_trace["marker"]["color"].append("red")
			elif(node in self.taking_inputs.keys()):
				node_trace["text"].append(node+">>"+str(self.taking_inputs[node]))
				node_trace["marker"]["color"].append("green")
			else:
				node_trace["text"].append(node)
				node_trace["marker"]["color"].append("blue")

		xmargin = 0.15
		ymargin = 0.4
		fig = Figure(data=Data([edge_trace, node_trace]),
					 layout=Layout(
					 	autosize=False,
    					width=500,
    					height=500,
						title='<br>Your Query DAG',
						# titlefont=dict(size=16),
						showlegend=False,
						hovermode='closest',
						margin=dict(b=20,l=5,r=5,t=40),
						xaxis=XAxis(showgrid=False, zeroline=False, range = [-5,5], showticklabels=False),
						yaxis=YAxis(showgrid=False, zeroline=False, range = [-5,5], showticklabels=False),
						shapes = [{'type': 'rect','x0': i-xmargin,'y0': -ymargin,'x1': i+0.25+xmargin,'y1': j+ymargin,'line': {'color': 'yellow',}} for i,j in rect]
						))

		# plot(fig, auto_open=False, filename='networkx.html')
		div = plot(fig, auto_open=False, output_type='div')
		print(div)
		return div


if __name__=="__main__":
	queries = {"q1":["query 1",["inp1","inp2"],["out1","out2"]],
			"q2":["query 2",["inp1"],["out1","out2"]],
			"q3":["query 3",["inp1","inp2","inp3"],["out1"]]}
	types = {"q1":"mongoDB","q2":"PostProcesing","q3":"neo4j"}
	dag = DAG(open("input_graph.txt","r"),queries,types)
	dag.generate_dag("my_dag")
