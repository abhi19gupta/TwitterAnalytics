from __future__ import print_function
import plotly.graph_objs as go
from plotly.graph_objs import *
from plotly.offline import plot
import os, time
import networkx as nx
import numpy as np
from pprint import *
##expect a dictionary of queries with keys as the query names/postprocessing function names
## for neo4j queries it will be the query code, input, output list
## for mongoDB queries it will be the partially formatted query specification, input, output list
## for post processing functions it will be the function_definition, input, output list
# queries = {"q1":[],"q2":[],...}
# types = {"q1":"neo4j","q2":"mongoDB","q3":"PostProcesing",....}

#Expect that there is a function to evaluate a query given its inputs(as a dictionary) and returns a dictionary of outputs
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
		self.returns = []
		self.inputs = {}
		self.connections = []
		self.taking_inputs = {}
		self.giving_outputs = []
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
				self.inputs[query_name]  ={}
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
					self.connections.append((self.graph.node[u]["query_name"]+"."+outp,self.graph.node[v]["query_name"]+"."+inp))
				elif(cond==0):
					l = line.strip().split()
					ind = l[0].index(".")
					node_name,var_name = l[0][:ind],l[0][ind+1:]
					self.taking_inputs[self.graph.node[node_name]["query_name"]+"."+var_name] = eval(l[1])
					self.inputs[self.graph.node[node_name]["query_name"]][var_name] = eval(l[1])
				elif(cond==2):
					s = line.strip()
					ind = s.index(".")
					node_name,var_name = s[:ind], s[ind+1:]
					self.giving_outputs.append(self.graph.node[node_name]["query_name"]+"."+var_name)
					self.returns.append((self.graph.node[node_name]["query_name"],var_name))

		# for q in self.inputs.keys():
		# 	for k,v in self.inputs[q]:
		# 		self.taking_inputs[q+"."+k] = v

		is_dag = nx.is_directed_acyclic_graph(self.graph)
		if(not(is_dag)):
			print("The input file doesn't specify a DAG. Please Check!!")
		pprint(self.graph.nodes(data=True))
		pprint(self.graph.edges(data=True))
		pprint(self.inputs)
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
			inputs = self.inputs[query_name]
			for inp in self.queries[query_name][1]:
				if inp not in inputs.keys():
					print("Node not getting all inputs")
					sys.exit(0)
			outputs = execute(query_name,inputs)
			self.outputs_dict[self.graph.node[node]["query_name"]] = outputs
			for nbr in self.graph.neighbors(node):
				outp, inp = self.graph.edge[node][nbr]["mapping"]
				self.inputs[self.graph.node[nbr]["query_name"]][inp] = outputs[outp]
		rets= {}
		for ret in self.returns:
			rets[ret] = self.outputs_dict[ret[0]][ret[1]]
		return rets
	def get_drawable_dag(self,G,queries,types,connections):
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
		for i,o in connections:
			dis.add_edge(i,o)
		return dis,rectangle


	def plot_dag(self):
		G ,rect= self.get_drawable_dag(self.graph,self.queries,self.types,self.connections)
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
	dag = DAG("input_graph.txt",queries,types)
