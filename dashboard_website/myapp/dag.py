from __future__ import print_function
import os, time
import networkx as nx
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
		self.graph = nx.DiGraph()
		self.returns = []
		self.inputs = {}
		fin = open(network_file,"r")
		cond = -1
		count = 0
		for line in fin:
			count+=1
			print(line)
			if(count==1):
				l = line.strip().split()
				self.num_nodes = int(l[0])
				# self.num_edges = int(l[1])
			elif(count<=1+self.num_nodes):
				node_name, query_name = line.strip().split()
				self.graph.add_node(node_name,type=self.types[query_name],
					query_name = query_name,query = self.queries[query_name][0])
				self.inputs[node_name]  ={}
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
					u,outp = l[0].split(".")
					v,inp = l[1].split(".")
					self.graph.add_edge(u,v,mapping=(outp,inp))
				elif(cond==0):
					l = line.strip().split()
					node_name,var_name = l[0].split(".")
					self.inputs[node_name][var_name] = eval(l[1])
				elif(cond==2):
					node_name,var_name = l[0].split(".")
					self.returns.append((node_name,var_name))
		is_dag = nx.is_directed_acyclic_graph(self.graph)
		if(not(is_dag)):
			print("The input file doesn't specify a DAG. Please Check!!")
		pprint(self.graph.nodes(data=True))
		pprint(self.graph.edges(data=True))
		pprint(self.inputs)
	def feed_forward(self):
		"""
		Do a topological sort and then do a bfs
		"""
		outputs_dict = {}
		ts = nx.topological_sort(self.graph)
		for node in t:
			query_name = G.nodes[node]["query_name"]
			inputs = self.inputs[query_name]
			for inp in self.queries[query_name][1]:
				if inp not in inputs.keys():
					print("Node not getting all inputs")
					sys.exit(0)
			outputs = execute(query_name,inputs)
			outputs_dict[node] = outputs
			for nbr in self.graph.neighbors(node):
				outp, inp = G.edge[node][nbr]
				self.inputs[nbr][inp] = outputs[outp]
		rets= {}
		for ret in self.returns:
			rets[ret] = outputs_dict[ret[0]][ret[1]]
		return rets

if __name__=="__main__":
	queries = {"q1":["query 1",["inp1","inp2"],["out1","out2"]],
			"q2":["query 2",["inp1"],["out1","out2"]],
			"q3":["query 3",["inp1","inp2","inp3"],["out1"]]}
	types = {"q1":"mongoDB","q2":"PostProcesing","q3":"neo4j"}
	dag = DAG("input_graph.txt",queries,types)
	


# def answer_query(request):
# 	query_s = ""
# 	output_s = ""
# 	if(request.method=="GET"):
# 		phform = PopularHash()
# 		phiform = CommomFollower()
# 		huiform = NewFollowers()
# 		hsiform = HashFolForm()
		
# 	else:
# 		if("b1" in request.POST):
# 			phform = PopularHash(request.POST)
# 			print("first button pressed")
# 			if phform.is_valid():
# 				name = phform.cleaned_data['Name']
			
# 		elif("b2" in request.POST):
# 			phiform = PopularHashInInterval(request.POST)
# 			print("second button pressed")
# 			if phiform.is_valid():
# 				pass
				
# 		elif("b3" in request.POST):
# 			huiform = HashUsageInInterval(request.POST)
# 			print("third button pressed")
# 			if huiform.is_valid():
# 				pass
				
# 		elif("b4" in request.POST):
# 			hsiform = HashSentimentInInterval(request.POST)
# 			print("4th button pressed")
# 			if hsiform.is_valid():
# 				pass
				

# 	phform = PopularHash()
# 	phiform = CommomFollower()
# 	huiform = NewFollowers()
# 	hsiform = HashFolForm()
# 	return render(request, 'mongo_queries.html', {"phform":phform, "phiform":phiform, "huiform":huiform, "hsiform":hsiform,})