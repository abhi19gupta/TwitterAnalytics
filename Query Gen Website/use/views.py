from __future__ import print_function
from django.shortcuts import render
from django.http import HttpResponse, HttpResponseRedirect
from django.contrib.auth.decorators import login_required
from django.template import loader
from django import forms
from django.urls import reverse
from .forms import *
from .models import *
import django_tables2 as tables
from datetime import datetime, timezone
from pprint import *
from pprint import *
import jinja2 as jj
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout, Fieldset, MultiField, ButtonHolder, Submit, HTML, Div, Row, Column
from .generate_queries import *
from django.forms.widgets import SelectDateWidget
from dateutil import parser


from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime
import json
import os
import time

match_s = """MATCH {% for aa in act_att %}({{aa[0]}} {{aa[1]}}),{% endfor %}"""
where_s = """WHERE {% for rel in rels %} ({{rel[0]}}) -[:{{rel[1]}}]-> ({{rel[2]}}) AND {% endfor %}"""
return_s  = """RETURN {% for po in prop_vars %} {{po[0]}}({{po[1]}}), {% endfor %}"""
total_s = """
{% for p in parts %}{{p}}
{% endfor %}
"""

def generate_simple_query(actors,attributes,relations,return_values):
    actors = [x[0]+" :"+x[1] for x in actors]
    attribs = [", ".join(["{"+x[i][0]+":"+str(x[i][1])+"}" for i in range(len(x))]) for x in attributes]
    act_att = zip(actors,attribs)

    m_template = jj.Template(match_s)
    m_code = m_template.render(act_att=act_att)
    w_template = jj.Template(where_s)
    w_code = w_template.render(rels=relations)
    r_template = jj.Template(return_s)
    r_code = r_template.render(prop_vars=return_values)

    m_code = m_code.rstrip().rstrip(",")
    w_code = w_code.rstrip().rstrip("AND")
    r_code = r_code.rstrip().rstrip(",")

    print(m_code)
    print(w_code)
    print(r_code)

    tot_template = jj.Template(total_s)
    tot_code = tot_template.render(parts=[m_code,w_code,r_code])
    return tot_code

# sq = generate_simple_query(actors=[("u1","USER"),("u2","USER"),("x","USER")],attributes=[[("id",101311381)],[("id",44196397)],[]],relations=[("x","FOLLOWS","u1"),("x","FOLLOWS","u2")],return_values=[("count","x")])

class UserForm(forms.Form):
    User_Variable  = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)
    UserId = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)

class TweetForm(forms.Form):
    Variable_Name  = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)
    Hashtag  = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)
    Retweet_Of = forms.ModelChoiceField(queryset=Tweet.objects.all(),required=False)
    Reply_Of = forms.ModelChoiceField(queryset=Tweet.objects.all(),required=False)
    Quoted = forms.ModelChoiceField(queryset=Tweet.objects.all(),required=False)
    Has_Mentioned = forms.ModelChoiceField(queryset=User.objects.all(),required=False)

class RelationForm(forms.Form):
    Source = forms.ModelChoiceField(queryset=User.objects.all(),required=False)
    URelationShip = forms.ChoiceField(choices=[(x,x) for x in [None,"FOLLOWS","STARTED_FOLLOWING","FOLLOWED"]],required=False)
    UDestination = forms.ModelChoiceField(queryset=User.objects.all(),required=False)
    
    Ut1 = forms.DateTimeField(required=False,widget=SelectDateWidget(years=("2016","2017")))
    Ut1m = forms.TimeField(widget=forms.TimeInput(format='%H:%M'),required=False)
    Ut2 = forms.DateTimeField(required=False,widget=SelectDateWidget(years=("2016","2017")))
    Ut2m = forms.TimeField(widget=forms.TimeInput(format='%H:%M'),required=False)

    TRelationShip = forms.ChoiceField(choices=[(x,x) for x in [None,"TWEETED"]],required=False)
    TDestination = forms.ModelChoiceField(queryset=Tweet.objects.all(),required=False)

    Tt1 = forms.DateTimeField(required=False,widget=SelectDateWidget(years=("2016","2017")))
    Tt1m = forms.TimeField(widget=forms.TimeInput(format='%H:%M'),required=False)

    Tt2 = forms.DateTimeField(required=False,widget=SelectDateWidget(years=("2016","2017")))
    Tt2m = forms.TimeField(widget=forms.TimeInput(format='%H:%M'),required=False)

class EvaluateForm(forms.Form):
    Eval_Variable  = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)
    Property = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)

class TweetTable(tables.Table):
    Variable_Name = tables.Column()
    Hashtag = tables.Column()
    Retweet_Of = tables.Column()
    Reply_Of = tables.Column()
    Quoted = tables.Column()
    Has_Mentioned = tables.Column()

    class Meta:
        attrs = {'class': 'paleblue','width':'200%'}

class UserTable(tables.Table):
    Variable_Name = tables.Column()
    UserId = tables.Column()
    class Meta:
        attrs = {'class': 'paleblue','width':'200%'}

class RelationTable(tables.Table):
    Source = tables.Column()
    Relation_Ship = tables.Column()
    Destination = tables.Column()
    Begin = tables.Column()
    End = tables.Column()
    class Meta:
        attrs = {'class': 'paleblue','width':'200%'}

class DummyForm(forms.Form):
    pass

########################################################### View functions  ###############################################################
def index(request):
    if(request.method=="GET"):
        dummy1 = DummyForm()
        dummy2 = DummyForm()

    elif("simple" in request.POST):
        print("came here")
        return HttpResponseRedirect(reverse("answer_query"))
    elif("custom" in request.POST):
        print("came here")
        return HttpResponseRedirect(reverse("query"))

    dummy1 = DummyForm()
    dummy2 = DummyForm()
    return render(request, 'use/index.html', {"dummy1":dummy1, "dummy2":dummy2})

def transform_time(t,tm):
    # print(parser.parse(str(t)[:5]+" "+str(tm)))
    return parser.parse(str(t)[:5]+" "+str(tm)).timestamp()

def query(request):
    output_s = ""
    query_s = ""
    if request.method == 'GET':
        uform = UserForm()
        tform = TweetForm()
        rform = RelationForm()
        eform = EvaluateForm()

    elif "b1" in request.POST:
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
            ut1m = rform.cleaned_data['Ut1m']

            ut2d = rform.cleaned_data['Ut2']
            ut2m = rform.cleaned_data['Ut2m']

            tt1d = rform.cleaned_data['Tt1']
            tt1m = rform.cleaned_data['Tt1m']

            tt2d = rform.cleaned_data['Tt2']
            tt2m = rform.cleaned_data['Tt2m']

            try:
                ut1 = transform_time(ut1d,ut1m)
            except:
                ut1 = ""
            try:
                ut2 = transform_time(ut2d,ut2m)
            except:
                ut2 = ""
            try:
                tt1 = transform_time(tt1d,tt1m)
            except:
                tt1 = ""
            try:
                tt2 = transform_time(tt2d,tt2m)
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
            var = eform.cleaned_data['Eval_Variable']
            prop = eform.cleaned_data['Property']
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
        sq = create_query(actors=users+tweets,attributes=uprops+tprops,relations=relations,return_values=[(prop,var)])
        print("the query is ",sq)
        query_s = sq
        
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
        session = driver.session()
        result = session.run(sq,{})
        for r in result:
            print(r)
            output_s += str(r)
        session.close()
        
        User.objects.all().delete()
        Tweet.objects.all().delete()
        Relation.objects.all().delete()

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

    uform = UserForm()
    tform = TweetForm()
    rform = RelationForm()
    eform = EvaluateForm()
    return render(request, 'use/query.html', {"query_s":query_s, 'output_s':output_s, 'uform': uform,'tform': tform,'rform': rform, 'eform' : eform, 'user_list':utable,'tweet_list':ttable,'relation_list':rtable,})



q1 = """match (u:USER)-[:TWEETED]->(t:TWEET)-[:HAS_HASHTAG]->(:HASHTAG {text:"{{h}}"}) with distinct u as u1 return count(u1)"""
q2 = """match (x:USER)-[:FOLLOWS]->(:USER {id:{{u1}}}), (x)-[:FOLLOWS]->(:USER {id:{{u2}}}) with distinct x as x1 return count(x1)"""
q3 = """
match (fe:FOLLOW_EVENT)-[:FE_FOLLOWED]->(u:USER {id:{{u}}}) 
where fe.timestamp > {{t1}} and fe.timestamp < {{t2}}
return count(fe)
"""
q4 = """
match (x:USER {id:{{u}}})-[:TWEETED]->(:TWEET)-[:HAS_HASHTAG]->(h:HASHTAG), (f:USER)-[:FOLLOWS]->(x), (f)-[:TWEETED]->(:TWEET)-[:HAS_HASHTAG]->(h) 
with distinct f as f1 
return count(f1)
"""
q5 = """
match (te:TWEET_EVENT)-[:TE_TWEET]->(:TWEET)-[:RETWEET_OF]->(t:TWEET), (te)-[:TE_USER]->(:USER {id:{{u}}}), (x:USER)-[:TWEETED]->(t) 
where te.timestamp < {{t1}} and te.timestamp > {{t2}} 
with distinct x as x1 
return count(x1)
"""
class SameHash(forms.Form):
    Hashtag  = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)

class CommomFollower(forms.Form):
    User1  = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)
    User2  = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)

class NewFollowers(forms.Form):
    User  = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)
    Begin_Date = forms.DateTimeField(required=False,widget=SelectDateWidget(years=("2016","2017")))
    Begin_Time = forms.TimeField(required=False,widget=forms.TimeInput(format='%H:%M'))

    End_Date = forms.DateTimeField(required=False,widget=SelectDateWidget(years=("2016","2017")))
    End_Time = forms.TimeField(required=False,widget=forms.TimeInput(format='%H:%M'))

class HashFolForm(forms.Form):
    User  = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)

class RetweetForm(forms.Form):
    User  = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)
    Begin_Date = forms.DateTimeField(required=False,widget=SelectDateWidget(years=("2016","2017")))
    Begin_Time = forms.TimeField(required=False,widget=forms.TimeInput(format='%H:%M'))

    End_Date = forms.DateTimeField(required=False,widget=SelectDateWidget(years=("2016","2017")))
    End_Time = forms.TimeField(required=False,widget=forms.TimeInput(format='%H:%M'))



def answer_query(request):
    query_s = ""
    output_s = ""
    if(request.method=="GET"):
        shform = SameHash()
        cf = CommomFollower()
        nf = NewFollowers()
        hf = HashFolForm()
        rfform = RetweetForm()
        dummy = DummyForm()

    elif("custom" in request.POST):
        print("came here")
        return HttpResponseRedirect(reverse("query"))
    else:
        if("b1" in request.POST):
            shform = SameHash(request.POST)
            print("first button pressed")
            if shform.is_valid():
                h = shform.cleaned_data['Hashtag']
            q_template = jj.Template(q1)
            q_code = q_template.render(h=h)
            query_s = q_code
        elif("b2" in request.POST):
            cfform = CommomFollower(request.POST)
            print("second button pressed")
            if cfform.is_valid():
                u1 = cfform.cleaned_data['User1']
                u2 = cfform.cleaned_data['User2']
                q_template = jj.Template(q2)
                q_code = q_template.render(u1=u1,u2=u2)
                query_s = q_code
        elif("b3" in request.POST):
            nfform = NewFollowers(request.POST)
            print("third button pressed")
            if nfform.is_valid():
                u = nfform.cleaned_data['User']
                bd = nfform.cleaned_data['Begin_Date']
                bt = nfform.cleaned_data['Begin_Time']
                ed = nfform.cleaned_data['End_Date']
                et = nfform.cleaned_data['End_Time']

                t1 = transform_time(bd,bt)
                t2 = transform_time(ed,et)

                q_template = jj.Template(q3)
                q_code = q_template.render(u=u,t1=t1,t2=t2)
                query_s = q_code
        elif("b4" in request.POST):
            hfform = HashFolForm(request.POST)
            print("4th button pressed")
            if hfform.is_valid():
                u = hfform.cleaned_data['User']
                q_template = jj.Template(q4)
                q_code = q_template.render(u=u)
                query_s = q_code

        elif("b5" in request.POST):
            rfform = NewFollowers(request.POST)
            print("5th button pressed")
            if rfform.is_valid():
                u = rfform.cleaned_data['User']
                bd = rfform.cleaned_data['Begin_Date']
                bt = rfform.cleaned_data['Begin_Time']
                ed = rfform.cleaned_data['End_Date']
                et = rfform.cleaned_data['End_Time']
                t1 = transform_time(bd,bt)
                t2 = transform_time(ed,et)

                q_template = jj.Template(q5)
                q_code = q_template.render(u=u,t1=t1,t2=t2)
                query_s = q_code
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
        session = driver.session()
        result = session.run(q_code,{})
        for r in result:
            print(r)
            output_s += str(r)
        session.close()

    shform = SameHash()
    cfform = CommomFollower()
    nfform = NewFollowers()
    hfform = HashFolForm()
    rfform = RetweetForm()
    dummy = DummyForm()
    return render(request, 'use/ans_query.html', {"dummy":dummy,"shform":shform, "cfform":cfform, "nfform":nfform, "hfform":hfform, "rfform":rfform, "query_s":query_s,"output_s":output_s})