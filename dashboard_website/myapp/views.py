from django.shortcuts import render, redirect, HttpResponse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib import messages
import django_tables2 as tables
from django.core.exceptions import ObjectDoesNotExist
from django.forms.models import model_to_dict
from material import *

from myapp.forms import *
from myapp.models import *
from myapp.tables import *
from myapp.create_dag import DAG
from myapp.flink.flink_code_gen import FlinkCodeGenerator
from myapp.flink.flink_api import FlinkAPI
from myapp.generate_queries import *
from myapp.mongo.ingest_raw import MongoQuery
from myapp.mongo_alert import MongoAlert

from datetime import datetime
from pprint import *
import json, os, copy

from neo4j.v1 import GraphDatabase, basic_auth, types

# TODOS:
# username in neo4j
# deal with multiple hashtags in create neo4j query form
# include post processing functions in create and execute DAG
# unions and AND

###################################################################################################
####################################  Create Global Objects #######################################
###################################################################################################
mongoQuery = MongoQuery()
mongoAlert = MongoAlert()
neo4jCreator = CreateQuery()
flinkCodeGenerator = FlinkCodeGenerator()
flink_api = FlinkAPI()


###################################################################################################
####################################  Meta logic functions ########################################
###################################################################################################
dag_div = ""
query_output  = ""
def binning(vals, num_bins):
	if len(vals) == 0:
		return ([],[])
	vals.sort(key=lambda x:x[0])
	print(vals)
	a,b = zip(*vals)
	length = max(a)+1-min(a)
	bin_length = length/num_bins
	print(bin_length)
	x = [min(a)+bin_length*i+bin_length/2 for i in range(num_bins)]
	x = [datetime.fromtimestamp(x1) for x1 in x]
	y = [0]*num_bins
	mappings = [int((val-min(a))/bin_length) for val in a]
	print(mappings)
	for i,m in enumerate(mappings):
		y[m]+=b[i]
	return (x,y)

def execute(query_name,inputs):
	q = Query.objects.get(name=query_name)
	outputs = [x.output_name for x in QueryOutput.objects.filter(query=q)]
	ret = {out:[] for out in outputs}
	print("========================================")
	print("Executing query ",q.name)
	pprint(inputs)

	if(q.type=='neo4j'):
		driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
		session = driver.session()
		result = session.run(q.query,inputs)
		ret = {x:[] for x in outputs}
		try:
			for record in result:
				for out in outputs:
					if(isinstance(record[out],bytes)):
						ret[out].append(record[out].decode("utf-8"))
					else:
						ret[out].append(record[out])
		except:
			pass

	elif(q.type=="mongoDB"):
		consts = QueryConstant.objects.filter(query=q)
		input_objs = QueryInput.objects.filter(query=q)
		temp = {i.attribute:inputs[i.input_name] for i in input_objs}
		temp.update({c.attribute:c.value for c in consts})
		if(q.query=="mp_ht_in_total"):
			ret = mongoQuery.mp_ht_in_total(**temp)
		elif(q.query=="mp_ht_in_total"):
			ret = mongoQuery.mp_ht_in_interval(**temp)
		if(q.query=="ht_in_interval"):
			ret = mongoQuery.ht_in_interval(**temp)
		if(q.query=="ht_with_sentiment"):
			ret = mongoQuery.ht_with_sentiment(**temp)

	elif(q.type=="postProcessing"):
		context = {"inputs":copy.deepcopy(inputs)}
		try:
			compile(q.query,'','exec')
			exec(q.query + "\n" + "ret = func(inputs)", context)
			for out in outputs:
				ret[out] = context[out]
		except Exception as e:
			print("Exeption while executing Post proc function: %s, %s"%(type(e),e))

	print(ret)
	print("========================================")
	return ret

def get_all_queries():
		ret_query = {}
		ret_types = {}
		queries = Query.objects.all()
		for query in queries:
			inputs = [x.input_name for x in QueryInput.objects.filter(query=query)]
			outputs = [x.output_name for x in QueryOutput.objects.filter(query=query)]
			ret_query[query.name] = [query.query, inputs, outputs]
			ret_types[query.name] = query.type
		return ret_query,ret_types


###################################################################################################
####################################  Website View Functions ######################################
###################################################################################################
def home(request):
   return redirect("hashtags")

def hashtags(request):
	if request.method == 'POST':
		print("POST request not supported at this route.")
		return
	return render(request, "myapp/hashtags.html", {"usage_form":HashtagForm(), "top10_form":Top10Form(), "sentiment_form":HashtagForm()})

def hashtag_usage_getter(request):
	if request.method == 'GET':
		print("GET request not supported at this route.")
		return

	form = HashtagForm(request.POST)
	if form.is_valid():
		hashtag = form.cleaned_data['hashtag']
		start_time = form.cleaned_data['start_time']
		end_time = form.cleaned_data['end_time']
		print(hashtag, start_time, end_time)
		data = mongoQuery.ht_in_interval(hashtag, start_time.timestamp(), end_time.timestamp())
		(x,y) = binning([(x,1) for x in data["timestamps"]],60)
		data = {"x":x,"y":y}
	else:
		print(form['hashtag'].errors, form['start_time'].errors, form['end_time'].errors)

	# data = {"x":[1,2,3,4], "y":[6,2,5,2]}
	return JsonResponse(data)

def hashtag_top10_getter(request):
	if request.method == 'GET':
		print("GET request not supported at this route.")
		return

	form = Top10Form(request.POST)
	if form.is_valid():
		start_time = form.cleaned_data['start_time']
		end_time = form.cleaned_data['end_time']
		print(start_time, end_time)
		data = mongoQuery.mp_ht_in_interval(20, start_time.timestamp(), end_time.timestamp())
		data = [{"hashtag":x[0],"count":x[1]} for x in list(zip(data["_id"],data["count"]))]
	else:
		print(form['start_time'].errors, form['end_time'].errors)

	# data = [{"hashtag":"Sports","count":132}, {"hashtag":"Politics","count":95}, {"hashtag":"Health","count":55}, {"hashtag":"Cricket","count":34}]
	return JsonResponse(data,safe=False)

def hashtag_sentiment_getter(request):
	if request.method == 'GET':
		print("GET request not supported at this route.")
		return

	form = HashtagForm(request.POST)
	if form.is_valid():
		hashtag = form.cleaned_data['hashtag']
		start_time = form.cleaned_data['start_time']
		end_time = form.cleaned_data['end_time']
		print(hashtag, start_time, end_time)
		data = mongoQuery.ht_with_sentiment(hashtag,start_time.timestamp(), end_time.timestamp())
		(x,y) = binning(list(zip(data["timestamps"],data["positive_sentiment"])),60)

		data = {"x":x,"y":y}
	else:
		print(form['hashtag'].errors, form['start_time'].errors, form['end_time'].errors)

	# data = {"x":[1,2,3,4], "y":[6,2,5,2]}
	return JsonResponse(data)

def query_creator(request):

	def get_create_query_subtab_data():
		users = []
		for usr in User.objects.all():
			users.append({"Variable_Name":usr.uname,"UserId":usr.userid})
		# utable = UserTable(d)
		tweets = []
		for twt in Tweet.objects.all():
			tweets.append({"Variable_Name":twt.tname,"Hashtag":twt.hashtag,"Retweet_Of":twt.retweet_of,"Reply_Of":twt.reply_of,"Quoted":twt.quoted,"Has_Mentioned":twt.has_mention})
		# ttable = TweetTable(d)

		rels = []
		for rel in Relation.objects.all():
			rels.append({"Source":rel.source,"Relation_Ship":rel.relation,"Destination":rel.destn,"Begin":rel.bt,"End":rel.et})
		# rtable = RelationTable(d)
		# print(d)
		# tables.RequestConfig(request).configure(utable)
		# tables.RequestConfig(request).configure(ttable)
		# tables.RequestConfig(request).configure(rtable)

		return {'uform': UserForm(),'tform': TweetForm(),'rform': RelationForm(), 'eform' : EvaluateForm(),
		'user_list':users,'tweet_list':tweets,'relation_list':rels}


	output_s = ""
	query_s = request.GET.get("query_s","")

	data = get_create_query_subtab_data()
	data.update({"create_mongo_form_1":PopularHash(), "create_mongo_form_2":PopularHashInInterval(),
		"create_mongo_form_3":HashUsageInInterval(), "create_mongo_form_4":HashSentimentInInterval()})
	data.update({"createpostprocform":PostProcForm()})
	data.update({"query_s":query_s, 'output_s':output_s})
	data.update({"createdagform":CreateDagForm()})
	data.update({"create_custom_metric_form":CreateCustomMetricForm()})
	data.update({"custom_metrics_form":CreateCustomMetricForm()})
	q_lis = []
	queries = Query.objects.all()
	for query in queries:
		q_dict = {}
		q_dict["name"] = query.name
		q_dict["query_type"] = query.type
		q_dict["code"] = query.query

		inputs = [x.input_name for x in QueryInput.objects.filter(query=query)]
		outputs = [x.output_name for x in QueryOutput.objects.filter(query=query)]
		q_dict["inputs"] = inputs
		q_dict["outputs"] = outputs
		q_lis.append(q_dict)
	pprint(q_lis)
	data.update({"queries":q_lis})
	data.update({"dags":Dag.objects.all()})
	data.update({"createcmform":CreateCustomMetricForm()})
	return render(request, 'myapp/create_query.html', data)

def create_neo4j_query_handler(request):

	sq = ""

	if "b1" in request.POST:
		uform = UserForm(request.POST)
		print("first button pressed")
		if uform.is_valid():
			vn = uform.cleaned_data['User_Variable']
			i = uform.cleaned_data['UserId']
			screen_name = uform.cleaned_data['User_Screen_Name']
			print("vn is ",vn)
			s = User.objects.create( uname= vn,userid=i,userscreenname=screen_name,username="")
	elif "b2" in request.POST:
		tform = TweetForm(request.POST)
		print("second button pressed")
		if tform.is_valid():
			n = tform.cleaned_data['Variable_Name']
			ht = tform.cleaned_data['Hashtag']
			retof = tform.cleaned_data['Retweet_Of']
			repof = tform.cleaned_data['Reply_Of']
			quo = tform.cleaned_data['Quoted']
			hasmen = tform.cleaned_data['Has_Mentioned']
			if(retof==None):
				retofs = ""
			else:
				retofs = retof.tname
			if(repof==None):
				repofs = ""
			else:
				repofs = retof.tname
			if(quo==None):
				quos = ""
			else:
				quos = quo.tname
			if(hasmen==None):
				hasmens = ""
			else:
				hasmens = hasmen.uname
			s = Tweet.objects.create(tname= n,hashtag=ht,retweet_of=retofs,reply_of=repofs,quoted=quos,has_mention=hasmens)
	elif "b3" in request.POST:
		rform = RelationForm(request.POST)
		print("third button pressed")
		if rform.is_valid():
			# pprint(rform.cleaned_data)
			src = rform.cleaned_data['Source']
			udst = rform.cleaned_data['UDestination']
			tdst = rform.cleaned_data['TDestination']
			urel = rform.cleaned_data['URelationShip']
			trel = rform.cleaned_data['TRelationShip']
			ut1d = rform.cleaned_data['Ut1']
			# ut1m = rform.cleaned_data['Ut1m']

			ut2d = rform.cleaned_data['Ut2']
			# ut2m = rform.cleaned_data['Ut2m']

			tt1d = rform.cleaned_data['Tt1']
			# tt1m = rform.cleaned_data['Tt1m']

			tt2d = rform.cleaned_data['Tt2']
			# tt2m = rform.cleaned_data['Tt2m']

			try:
				# ut1 = transform_time(ut1d,ut1m)
				ut1 = ut1d.timestamp()
			except:
				ut1 = ""
			try:
				# ut2 = transform_time(ut2d,ut2m)
				ut2 = ut2d.timestamp()
			except:
				ut2 = ""
			try:
				# tt1 = transform_time(tt1d,tt1m)
				tt1 = tt1d.timestamp()
			except:
				tt1 = ""
			try:
				# tt2 = transform_time(tt2d,tt2m)
				tt2 = tt2d.timestamp()
			except:
				tt2 = ""

			print("the values of times are ",ut1,ut2,tt1,tt2)

			if src!=None and urel!=None and udst!=None:
				s = Relation.objects.create( source= src.uname,relation=urel,destn=udst.uname,bt=ut1,et=ut2)
			if src!=None and trel!="" and tdst!=None:
				print(trel)
				s = Relation.objects.create( source= src.uname,relation=trel,destn=tdst.tname,bt=tt1,et=tt2)
	elif "submit" in request.POST:
		print("came into submit condition ")
		users = []
		uprops = []
		tweets = []
		tprops = []
		relations = []
		eform = EvaluateForm(request.POST)
		if eform.is_valid():
			ret_vars = eform.cleaned_data['Return_Variables']
			query_name = eform.cleaned_data['Query_Name']

		if (len(Query.objects.filter(name=query_name)) > 0):
			print("Error: Query Name already exists!")
			messages.error(request, "Error: Query name already exists! Use a unique query name.")
		else:
			for u in User.objects.all():
				users.append((u.uname,"USER"))
				up = []
				if u.userid!="":
					up.append(("id",u.userid))
				if u.username!="":
					up.append(("name",u.username))
				if u.userscreenname!="":
					up.append(("screen_name",u.userscreenname))
				uprops.append(up)
			for t in Tweet.objects.all():
				tweets.append((t.tname,"TWEET"))
				tp = []
				if t.hashtag!="":
					tp.append(("hashtag",t.hashtag))
				if t.retweet_of!="":
					tp.append(("retweet_of",t.retweet_of))
				if t.reply_of!="":
					tp.append(("reply_of",t.reply_of))
				if t.quoted!="":
					tp.append(("quoted",t.quoted))
				if t.has_mention!="":
					tp.append(("has_mention",t.has_mention))
				tprops.append(tp)
			for r in Relation.objects.all():
				relations.append((r.source,r.relation,r.destn,r.bt,r.et))
			pprint(users)
			pprint(uprops)
			pprint(tweets)
			pprint(tprops)
			pprint(relations)
			sq = neo4jCreator.create_query(actors=users+tweets,attributes=uprops+tprops,relations=relations,return_values=ret_vars)
			print("the query is ",sq["code"])

			query_object = Query.objects.create(name=query_name, query=sq["code"], type='neo4j')
			for input_name in sq["inputs"]:
				QueryInput.objects.create(query=query_object, input_name=input_name)
			for output_name in sq["outputs"]:
				QueryOutput.objects.create(query=query_object, output_name=output_name)

			# driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
			# session = driver.session()
			# result = session.run(sq,{})
			# for r in result:
			#     print(r)
			#     output_s += str(r)
			# session.close()

			User.objects.all().delete()
			Tweet.objects.all().delete()
			Relation.objects.all().delete()

	return redirect("/create_query/?query_s=%s"%sq)

def create_mongo_query_handler(request):

	if("b1" in request.POST):
		phform = PopularHash(request.POST)
		print("first button pressed")
		if phform.is_valid():
			name = phform.cleaned_data["query_name"]
			number = phform.cleaned_data["number"]
			q = Query.objects.create(name=name,query="mp_ht_in_total",type="mongoDB")
			QueryOutput.objects.create(query=q,output_name="_id")
			QueryOutput.objects.create(query=q,output_name="count")
			if(number[0]=="{" and number[-1]=="}"):
				QueryInput.objects.create(query=q,attribute="limit",input_name=number[1:-1])
			else:
				QueryConstant.objects.create(query=q,attribute="limit",value=number)


	elif("b2" in request.POST):
		phiform = PopularHashInInterval(request.POST)
		print("second button pressed")
		if phiform.is_valid():
			name = phiform.cleaned_data["query_name"]
			number = phiform.cleaned_data["number"]
			begin_time = phiform.cleaned_data["begin_time"]
			end_time = phiform.cleaned_data["end_time"]
			q = Query.objects.create(name=name,query="mp_ht_in_interval",type="mongoDB")
			QueryOutput.objects.create(query=q,output_name="_id")
			QueryOutput.objects.create(query=q,output_name="count")
			if(number[0]=="{" and number[-1]=="}"):
				QueryInput.objects.create(query=q,attibute="limit",input_name=number[1:-1])
			else:
				QueryConstant.objects.create(query=q,attibute="limit",value=number)
			QueryConstant.objects.create(query=q,attibute="begin",value=str(begin_time.timestamp()))
			QueryConstant.objects.create(query=q,attibute="end",value=str(end_time.timestamp()))


	elif("b3" in request.POST):
		huiform = HashUsageInInterval(request.POST)
		print("third button pressed")
		if huiform.is_valid():
			name = huiform.cleaned_data["query_name"]
			hashtag = huiform.cleaned_data["hashtag"]
			begin_time = huiform.cleaned_data["begin_time"]
			end_time = huiform.cleaned_data["end_time"]
			q = Query.objects.create(name=name,query="mp_ht_in_interval",type="mongoDB")
			QueryOutput.objects.create(query=q,output_name="timestamps")
			if(hashtag[0]=="{" and hashtag[-1]=="}"):
				QueryInput.objects.create(query=q,attibute="hashtag",input_name=hashtag[1:-1])
			else:
				QueryConstant.objects.create(query=q,attibute="hashtag",value=hashtag)
			QueryConstant.objects.create(query=q,attibute="begin",value=str(begin_time.timestamp()))
			QueryConstant.objects.create(query=q,attibute="end",value=str(end_time.timestamp()))

	elif("b4" in request.POST):
		hsiform = HashSentimentInInterval(request.POST)
		print("4th button pressed")
		if hsiform.is_valid():
			name = hsiform.cleaned_data["query_name"]
			hashtag = hsiform.cleaned_data["hashtag"]
			begin_time = hsiform.cleaned_data["begin_time"]
			end_time = hsiform.cleaned_data["end_time"]
			q = Query.objects.create(name=name,query="mp_ht_in_interval",type="mongoDB")
			QueryOutput.objects.create(query=q,output_name="timestamps")
			QueryOutput.objects.create(query=q,output_name="positive_sentiment")
			QueryOutput.objects.create(query=q,output_name="negative_sentiment")
			if(hashtag[0]=="{" and hashtag[-1]=="}"):
				QueryInput.objects.create(query=q,attibute="hashtag",input_name=hashtag[1:-1])
			else:
				QueryConstant.objects.create(query=q,attibute="hashtag",value=hashtag)
			QueryConstant.objects.create(query=q,attibute="begin",value=str(begin_time.timestamp()))
			QueryConstant.objects.create(query=q,attibute="end",value=str(end_time.timestamp()))

	return redirect("/create_query/")

def create_postprocessing_handler(request):
	if request.method == 'POST':
		form = PostProcForm(request.POST, request.FILES)
		if form.is_valid():
			name = form.cleaned_data['name']
			inputs = form.cleaned_data['input_variable_names']
			input_variable_names = [x.strip() for x in inputs.split(",")]
			outputs = form.cleaned_data['output_variable_names']
			output_variable_names = [x.strip() for x in outputs.split(",")]
			code = request.FILES['file'].read()
			print(code)
			q = Query.objects.create(name=name,query=code,type="postProcessing")
			for input_var in input_variable_names:
				QueryInput.objects.create(query=q,input_name=input_var)
			for output_var in output_variable_names:
				QueryOutput.objects.create(query=q,output_name=output_var)
			return redirect("/create_query")

def create_custom_metric_handler(request):
	if request.method == 'POST':
		form = CreateCustomMetricForm(request.POST)
		if form.is_valid():
			CustomMetric.objects.create(name=form.cleaned_data['name'],query=form.cleaned_data['query'],post_proc=form.cleaned_data['post_processing_function'])
			return redirect("/create_query")

def view_custom_metric_handler(request):

	def convert_result_to_json(result):

		def tuple_list_to_json(tuple_list):
			dict_ = {}
			for (key,val) in tuple_list:
				if type(val) in [types.Node, types.Relationship]:
					val = tuple_list_to_json(val.items())
				dict_[key] = val
			return dict_

		ret = []
		for r in result:
			dict_ = tuple_list_to_json(r.items())
			ret.append(dict_)
		return ret

	def run_query(dag_name):
		# driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
		# session = driver.session()
		# result = session.run(query,{})
		# session.close()
		# return convert_result_to_json(result)
		dag_obj = Dag.objects.get(dag_name=dag_name)
		queries,types = get_all_queries()
		dag = DAG(dag_obj.source, queries, types)
		outputs = dag.feed_forward(execute)
		return outputs

	if request.method == 'POST':
		form = CreateCustomMetricForm(request.POST)
		if form.is_valid():
			dag = form.cleaned_data["DAG"]
			post_proc = form.cleaned_data["post_processing_function"].query
			input_args = [x.lstrip().rstrip() for x in form.cleaned_data["input_arguments"].split(",")]
			x_axis_output_field = form.cleaned_data["x_axis_output_field"].lstrip().rstrip()
			y_axis_output_field = form.cleaned_data["y_axis_output_field"].lstrip().rstrip()

			outputs = run_query(dag.dag_name)
			output_vars = {}
			for k,v in outputs.items():
				for var,val in v.items():
					output_vars[k+"."+var] = val

			context = {}
			for arg in input_args:
				tmp = [x.lstrip().rstrip() for x in arg.split("=")]
				name = tmp[0]
				val = tmp[1]
				if val in output_vars.keys():
					context[name] = output_vars[val]
				else:
					context[name] = eval(val)
			context = {"inputs":context}

			try:
				compile(post_proc,'','exec')
				exec(post_proc + "\n" + "ret = func(inputs)", context)
				(x_values,y_values) = (context["ret"][x_axis_output_field],context["ret"][y_axis_output_field])
				# print(context["x_values"],context["y_values"])
			except Exception as e:
				print("Error: ", type(e), e)
			data = {"x":x_values,"y":y_values}
			return JsonResponse(data)
		else:
			print(form.errors)


@csrf_exempt
def view_query_handler(request):
	if request.method == 'POST':
		query = request.POST['query']
		query = Query.objects.get(name=query)
		return JsonResponse({"query":query.query})

@csrf_exempt
def delete_query_handler(request,query):
	if request.method == 'POST':
		# query = request.POST["query"]
		query = Query.objects.get(name=query)
		query.delete()
		return redirect("/create_query")

@csrf_exempt
def view_post_proc_handler(request):
	if request.method == 'POST':
		post_proc = request.POST["post_proc"]
		post_proc = PostProcFunc.objects.get(name=post_proc)
		return JsonResponse({"post_proc":post_proc.code})

@csrf_exempt
def delete_post_proc_handler(request):
	if request.method == 'POST':
		post_proc = request.POST["post_proc"]
		post_proc = PostProcFunc.objects.get(name=post_proc)
		post_proc.delete()
		return JsonResponse({"url":"/create_metric"})


def alerts(request):

	def get_live_alerts():
		# alerts = [{"_id":1, "name":"alert1", "window_start":datetime.now(), "window_end":datetime.now(), "tweet_count":987},
		# {"_id":2, "name":"alert2", "window_start":datetime.now(), "window_end":datetime.now(), "tweet_count":789}]
		alerts = mongoAlert.get_all_alerts()
		for alert in alerts:
			alert['id'] = str(alert['id'])
			alert['tweet_count'] = len(alert['tweet_ids'].split(' '))
			alert['window_start'] = str(alert['window_start'])
			alert['window_end'] = str(alert['window_end'])
		# print(alerts)
		return alerts

	def get_alert_specs():
		alertSpecs = AlertSpecification.objects.all()
		flink_status = flink_api.check_job_status_all()
		# print(flink_status)
		ret = []
		for alertSpec in alertSpecs:
			ret.append({"id":alertSpec.id, "alert_name":alertSpec.alert_name, "status":flink_status.get(alertSpec.alert_name,'NOT RUNNING')})
		return ret

	live_alerts = get_live_alerts()
	alertSpecs = get_alert_specs()
	return render(request, "myapp/alerts.html", {"create_alert_form":CreateAlertForm(), "alerts":live_alerts, "alertSpecs":alertSpecs})

def alerts_view(request):
	alert_id = request.GET["alert_id"]
	try:
		alert = AlertSpecification.objects.get(id=alert_id)
		return HttpResponse("<pre>%s</pre>"%(json.dumps(model_to_dict(alert),indent=4)))
	except Exception as e:
		print('Alert not found! %s:%s'%(str(type(e)),str(e)))
		return HttpResponse('Alert not found!')

def alerts_delete(request):
	alert_id = request.GET["alert_id"]
	try:
		alert = AlertSpecification.objects.get(id=alert_id)
		flinkCodeGenerator.delete_code(alert.alert_name)
		try:
			flink_api.cancel_job(alert.current_job_id)
		except Exception as e:
			print("Failed to cancel_job. %s, %s"%(str(type(e)),str(e)))
		alert.delete()
	except Exception as e:
		print("Failed to delete alert. %s: %s"%(str(type(e),str(e))))
		messages.error(request, "Failed to delete alert. %s: %s"%(str(type(e),str(e))))
	return redirect('/alerts')

def alerts_activate(request):
	alert_id = request.GET["alert_id"]
	try:
		alertSpec = AlertSpecification.objects.get(id=alert_id)
	except Exception as e:
		messages.error(request, "Alert not found!.")
	try:
		print('Running jar')
		job_id = flink_api.run_jar(alertSpec.alert_name, alertSpec.flink_jar_id)
		alertSpec.current_job_id = job_id
		alertSpec.save()
	except Exception as e:
		try:
			print('Uploading jar')
			flink_jar_id = flink_api.upload_jar(alertSpec.jar_path)
			print('Running jar')
			job_id = flink_api.run_jar(alertSpec.alert_name,flink_jar_id)
			alertSpec.flink_jar_id = flink_jar_id
			alertSpec.current_job_id = job_id
			alertSpec.save()
		except Exception as e:
			print("Failed to activate alert. %s:%s"%(str(type(e),str(e))))
			messages.error(request, "Failed to activate alert. %s:%s"%(str(type(e),str(e))))
	return redirect('/alerts')

def alerts_deactivate(request):
	alert_id = request.GET["alert_id"]
	try:
		alertSpec = AlertSpecification.objects.get(id=alert_id)
	except Exception as e:
		messages.error(request, "Alert not found!.")
	try:
		flink_api.cancel_job(alertSpec.current_job_id)
	except Exception as e:
		print("Failed to deactivate job. %s:%s"%(str(type(e),str(e))))
		messages.error(request, "Failed to deactivate job. %s:%s"%(str(type(e),str(e))))
	return redirect('/alerts')

def alerts_tweets(request):
	alert_id = request.GET["alert_id"]
	alert = mongoAlert.get_alert(alert_id)
	if(alert is not None):
		display = "<br>".join([tweet_id for tweet_id in alert['tweet_ids'].split(' ')])
		return HttpResponse(display)
	else:
		return HttpResponse('Alert not found!')

def alerts_dismiss(request):
	alert_id = request.GET["alert_id"]
	mongoAlert.delete_alert(alert_id)
	return redirect('/alerts')

def alerts_create_handler(request):
	create_alert_form = CreateAlertForm(request.POST)
	if create_alert_form.is_valid():
		data = create_alert_form.cleaned_data
		# print(form_data)
		# first check that same alert_name doesn't exist already
		try:
			if AlertSpecification.objects.get(alert_name=data['alert_name']) is not None:
				messages.error(request, "This alert name (%s) already exists!"%(data['alert_name']))
				return redirect("/alerts/")
		except ObjectDoesNotExist:
			pass

		try:
			print('Writing code')
			flinkCodeGenerator.write_code(data['alert_name'],data['filter'],data['keys'],data['window_length'],
				data['window_slide'],data['count_threshold'])
			print('Compiling code')
			jar_path = flinkCodeGenerator.compile_code(data['alert_name'])
			print('Uploading jar')
			flink_jar_id = flink_api.upload_jar(jar_path)
			print('Running jar')
			job_id = flink_api.run_jar(data['alert_name'],flink_jar_id)
			AlertSpecification.objects.create(alert_name=data['alert_name'], filter=data['filter'],
				keys=','.join(data['keys']), window_length=data['window_length'], window_slide=data['window_slide'],
				count_threshold=data['count_threshold'], jar_path=jar_path, flink_jar_id=flink_jar_id, current_job_id=job_id)
		except Exception as e:
			print("Alert creation failed. Error: %s, %s"%(str(type(e)),str(e)))
			messages.error(request, "Alert creation failed. Error: %s, %s"%(str(type(e)),str(e)))
			flinkCodeGenerator.delete_code(data['alert_name'])
	else:
		messages.error(request, "Error: Invalid form data.")
	return redirect("/alerts/")


def create_query(request):
	global dag_div
	print(dag_div)
	print('create_query')
	return render(request, "myapp/create_query.html", {"uploadfileform":UploadFileForm(),"dag_graph_my":dag_div,"query_output":query_output})



def create_dag_handler(request):
	global dag_div
	global query_output
	print("came into the function")

	if request.method == 'POST':
		print("came into post")
		form = CreateDagForm(request.POST, request.FILES)
		if form.is_valid():
			print("came here")
			dag_name = form.cleaned_data["name"]
			description = form.cleaned_data["description"]

			file_handler = request.FILES['file']
			source = file_handler.read()

			queries,types = get_all_queries()
			dag = DAG(source, queries, types)
			dag_div = dag.plot_dag()
			Dag.objects.create(dag_name=dag_name,description = description, dag_div=dag_div,source=source)
			rets = dag.generate_dag(dag_name)
		else:
			print(form["name"].errors,form['file'].errors)
		return redirect('/create_query/')

def delete_dag_handler(request,dag_name):
	dag = Dag.objects.filter(dag_name=dag_name)[0]
	dag.delete()
	try:
		os.remove("myapp/airflow/dags/"+dag_name+".py")
	except OSError:
		pass
	os.system("python myapp/airflow/delete_dag.py "+dag_name)
	return redirect('/create_query/')

def view_dag_handler(request,dag_name):
	dag = Dag.objects.get(dag_name=dag_name)
	data = {"dag":dag}
	return render(request, 'myapp/view_dag.html', data)
