from __future__ import print_function
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



class UserForm(forms.Form):
    User_Variable  = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)
    UserId = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)

class TweetForm(forms.Form):
    Variable_Name  = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)
    Hashtag  = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}),required=False)

class RelationForm(forms.Form):
    Source = forms.ModelChoiceField(queryset=User.objects.all(),required=False)
    URelationShip = forms.ChoiceField(choices=[(x,x) for x in ["FOLLOWS"]])
    TRelationShip = forms.ChoiceField(choices=[(x,x) for x in ["TWEETED"]])
    UDestination = forms.ModelChoiceField(queryset=User.objects.all(),required=False)
    TDestination = forms.ModelChoiceField(queryset=Tweet.objects.all(),required=False)

# class UTRelationForm(forms.Form):
#     Source = forms.ModelChoiceField(queryset=User.objects.all(),required=False)
#     Destination = forms.ModelChoiceField(queryset=User.objects.all(),required=False)

class TweetTable(tables.Table):
    Variable_Name = tables.Column()
    Hashtag = tables.Column()
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
    class Meta:
        attrs = {'class': 'paleblue','width':'200%'}

def index(request):
    return HttpResponse("Hello there! welcome to swarna pg home page")

def query(request):
    if request.method == 'GET':
        uform = UserForm()
        tform = TweetForm()
        rform = RelationForm()

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
            s = Tweet.objects.create( tname= n,hashtag=ht)
    elif "b3" in request.POST:
        rform = RelationForm(request.POST)
        print("third button pressed")
        if rform.is_valid():
            pprint(rform.cleaned_data)
            src = rform.cleaned_data['Source']
            udst = rform.cleaned_data['UDestination']
            tdst = rform.cleaned_data['TDestination']
            urel = rform.cleaned_data['URelationShip']
            trel = rform.cleaned_data['TRelationShip']
            if src!=None and urel!=None and udst!=None:
                s = Relation.objects.create( source= src.uname,relation=urel,destn=udst.uname)
            if src!=None and trel!=None and tdst!=None:
                s = Relation.objects.create( source= src.uname,relation=trel,destn=tdst.tname)
    d = []
    for usr in User.objects.all():
        d.append({"Variable_Name":usr.uname,"UserId":usr.userid})
    utable = UserTable(d)
    d = []
    for twt in Tweet.objects.all():
        d.append({"Variable_Name":twt.tname,"Hashtag":twt.hashtag})
    ttable = TweetTable(d)

    d = []
    for rel in Relation.objects.all():
        d.append({"Source":rel.source,"Relation_Ship":rel.relation,"Destination":rel.destn})
    rtable = RelationTable(d)
    print(d)
    tables.RequestConfig(request).configure(utable)
    tables.RequestConfig(request).configure(ttable)
    tables.RequestConfig(request).configure(rtable)

    uform = UserForm()
    tform = TweetForm()
    rform = RelationForm()
    return render(request, 'use/query.html', {'uform': uform,'tform': tform,'rform': rform,'user_list':utable,'tweet_list':ttable,'relation_list':rtable,})
