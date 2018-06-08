import matplotlib.pyplot as plt
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import numpy as np
from pprint import *

def take_diff(l):
	l = [int(x) for x in l]
	temp = [l[i]-l[i-1] for i in range(1,len(l))]
	ans = []
	prev = 5
	for x in temp:
		ans.append(prev)
		prev+=x
	return ans
def plot_mongo_ingestion():
	fin  = open("debug_logs.txt","r")
	x = []
	y = []
	for line in fin:
		s = line.strip()
		if(s[:5]=="DEBUG"):
			l = s.split()
			x.append(l[2])
			y.append(l[5])
	pprint(y[:13])
	pprint(y[15:15+17])
	pprint(take_diff(x[:13]))
	# Create a trace
	trace = go.Scatter(
	    y = take_diff(x[:13]),
	    x = y[:13],
	    mode = "lines+markers",
	    name = "Without parallelization"
	)
	trace1 = go.Scatter(
	    y = take_diff(x[15:15+17]),
	    x = y[15:15+17],
	    mode = "lines+markers",
	    name = "With parallelization"
	)
	layout = go.Layout(
	    title='MongoDB Ingestion Rate',
	    xaxis=dict(
	        title='Number of tweets',
	    ),
	    yaxis=dict(
	        title='Time(seconds)',
	    )
	)
	data = [trace,trace1]
	fig = go.Figure(data=data, layout=layout)

	plot(fig, filename='graphs/mongo_ingestion')
##500 0.001478281859854533
##1000 0.0014121203765370492
##2000 0.0013005508262860146
##5000 0.0012475641345977783
##8000  0.0012029457139341455
x = [1,500,1000,2000,5000,8000,10000,15000,20000,35000,45000,50000]
y = [0.05,0.001478281859854533,0.0014121203765370492,0.0013005508262860146,0.0012475641345977783,0.0012029457139341455,0.00119100621064504,0.0011228984769185385,0.001119251218863896,0.001013281033720289,0.0012292806895573933,0.0013292806895573933]
def get_neo4j_logs_data(file):
	fin = open(file,"r")
	l = []
	for line in fin:
		l.append(float(line.strip().split()[3]))
	return l
def plot_neo4j_ingestion():
	ys = [1/a for a in y]
	pprint(ys)
	trace = go.Scatter(
	    y = ys,
	    x = x,
	    mode = "lines+markers",
	    name = "Without parallelization"
	)
	layout = go.Layout(
	    title='Neo4j Ingestion Rate',
	    xaxis=dict(
	        title='Transaction size',
	    ),
	    yaxis=dict(
	        title='Rate(tweets/sec)',
	    )
	)
	data = [trace]
	fig = go.Figure(data=data, layout=layout)

	plot(fig, filename='graphs/mongo_ingestion')
# l = get_neo4j_logs_data("neo4j_logs.txt")

# print(sum(l)/len(l))
# plot_neo4j_ingestion()

def plot_query_answering_rate():
	# num_procs = [1,2,3,4,5,6,7,8,9,10]
	# rate = [585.33, 852.04, 1016.82, 1093.76, 993.98, 981.92, 983.46, 969.79, 949.89, 941.74]
	num_procs = [1,2,3,4,5,6,7,8,10,12,14]
	rate = [12.7, 23.48, 36.37, 47.46, 53.3, 54.67, 61.15, 63.34, 65.45, 59.34, 56.12]
	trace = go.Scatter(
	    y = rate,
	    x = num_procs,
	    mode = "lines+markers",
	)
	layout = go.Layout(
	    title='Query Answering Rate for complex queries',
	    xaxis=dict(
	        title='Number of parallel procs firing queries',
	    ),
	    yaxis=dict(
	        title='Rate(queries/sec)',
	    )
	)
	data = [trace]
	fig = go.Figure(data=data, layout=layout)

	plot(fig, filename='graphs/query_answering_complex')

plot_query_answering_rate()