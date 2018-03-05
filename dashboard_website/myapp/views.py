from django.shortcuts import render, redirect
from django.http import JsonResponse
from myapp.forms import *
from myapp.ingest_raw import Query
from datetime import datetime
from myapp.tables import *

from django.shortcuts import render
from django.http import HttpResponse, HttpResponseRedirect
from django.contrib.auth.decorators import login_required
from django.template import loader
from django import forms
from django.core.urlresolvers import reverse
from .forms import *
from .models import *
import django_tables2 as tables
from datetime import datetime, timezone
from pprint import *
from pprint import *
import jinja2 as jj
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout, Fieldset, MultiField, ButtonHolder, Submit, HTML, Div, Row, Column
from myapp.generate_queries import *
from django.forms.extras.widgets import SelectDateWidget
from dateutil import parser


from neo4j.v1 import GraphDatabase, basic_auth, types
from datetime import datetime
import json
import os
import time

q = Query()

def binning(vals, num_bins):
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
        data = q.ht_in_interval(start_time.timestamp(), end_time.timestamp(), hashtag)
        (x,y) = binning([(x,1) for x in data],60)
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
        data = q.mp_ht_in_interval(start_time.timestamp(), end_time.timestamp())
        data = [{"hashtag":x["_id"],"count":x["count"]} for x in data]
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
        data = q.ht_with_sentiment(start_time.timestamp(), end_time.timestamp(), hashtag)
        (x,y) = binning([(x[0],x[1]) for x in data],60)
        data = {"x":x,"y":y}
    else:
        print(form['hashtag'].errors, form['start_time'].errors, form['end_time'].errors)
    
    # data = {"x":[1,2,3,4], "y":[6,2,5,2]}
    return JsonResponse(data)

def query_creator(request):

    def get_create_query_subtab_data():
        d = []
        for usr in User.objects.all():
            d.append({"Variable_Name":usr.uname,"UserId":usr.userid})
        utable = UserTable(d)
        d = []
        for twt in Tweet.objects.all():
            d.append({"Variable_Name":twt.tname,"Hashtag":twt.hashtag,"Retweet_Of":twt.retweet_of,"Reply_Of":twt.reply_of,"Quoted":twt.quoted,"Has_Mentioned":twt.has_mention})
        ttable = TweetTable(d)

        d = []
        for rel in Relation.objects.all():
            d.append({"Source":rel.source,"Relation_Ship":rel.relation,"Destination":rel.destn,"Begin":rel.bt,"End":rel.et})
        rtable = RelationTable(d)
        # print(d)
        tables.RequestConfig(request).configure(utable)
        tables.RequestConfig(request).configure(ttable)
        tables.RequestConfig(request).configure(rtable)

        return {'uform': UserForm(),'tform': TweetForm(),'rform': RelationForm(), 'eform' : EvaluateForm(), 
        'user_list':utable,'tweet_list':ttable,'relation_list':rtable}


    output_s = ""
    query_s = request.GET.get("query_s","")

    data = get_create_query_subtab_data()
    data.update({"query_s":query_s, 'output_s':output_s})
    data.update({"uploadfileform":UploadFileForm()})
    data.update({"create_custom_metric_form":CreateCustomMetricForm()})
    data.update({"custom_metrics_form":CustomMetricForm()})

    return render(request, 'myapp/custom_metrics.html', data)

def create_query_handler(request):

    sq = ""

    if "b1" in request.POST:
        uform = UserForm(request.POST)
        print("first button pressed")
        if uform.is_valid():
            vn = uform.cleaned_data['User_Variable']
            i = uform.cleaned_data['UserId']
            print("vn is ",vn)
            s = User.objects.create( uname= vn,userid=i)
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
        for u in User.objects.all():
            users.append((u.uname,"USER"))
            up = []
            if u.userid!="":
                up.append(("id",u.userid))
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
        sq = create_query(actors=users+tweets,attributes=uprops+tprops,relations=relations,return_values=ret_vars)
        print("the query is ",sq)

        Query.objects.create(name=query_name, query=sq)        
        
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

    return redirect("/create_metric/?query_s=%s"%sq)

def create_postprocessing_handler(request):
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            code = request.FILES['file'].read()
            print(code)
            PostProcFunc.objects.create(name=form.cleaned_data['name'],code=code)
            return redirect("/create_metric")

def create_custom_metric_handler(request):
    if request.method == 'POST':
        form = CreateCustomMetricForm(request.POST)
        if form.is_valid():
            CustomMetric.objects.create(name=form.cleaned_data['name'],query=form.cleaned_data['query'],post_proc=form.cleaned_data['post_processing_function'])
            return redirect("/create_metric")

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

    def run_query(query):
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
        session = driver.session()
        result = session.run(query,{})
        session.close()
        return convert_result_to_json(result)

    if request.method == 'POST':
        form = CustomMetricForm(request.POST)
        if form.is_valid():
            custom_metric = form.cleaned_data['metric']
            query = custom_metric.query.query
            post_proc = custom_metric.post_proc.code
            print("Query: ", query)
            print("Post-Proc: ", post_proc)
            query_result = run_query(query)
            # print(query_result)
            try:
                exec(post_proc,{"result":query_result})
            except Exception as e:
                print("Error: ", e)
            return redirect("/create_metric")