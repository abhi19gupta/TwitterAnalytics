from django import forms
from myapp.models import *
from django.forms.extras.widgets import SelectDateWidget

date_formats = ['%Y-%m-%dT%H:%M'] # this is returned by browsers implementing datetime-local 
date_formats.extend([
 '%Y-%m-%d %H:%M:%S',    # '2006-10-25 14:30:59'
 '%Y-%m-%d %H:%M',       # '2006-10-25 14:30'
 '%Y-%m-%d',             # '2006-10-25'
 '%m/%d/%Y %H:%M:%S',    # '10/25/2006 14:30:59'
 '%m/%d/%Y %H:%M',       # '10/25/2006 14:30'
 '%m/%d/%Y',             # '10/25/2006'
 '%m/%d/%y %H:%M:%S',    # '10/25/06 14:30:59'
 '%m/%d/%y %H:%M',       # '10/25/06 14:30'
 '%m/%d/%y',             # '10/25/06' -- till here was the default django list, next are custom
 '%d/%m/%Y %H:%M',       # '25/10/2016 14:30'
 '%d-%m-%Y %H:%M']        # '25-10-2016 14:30'      
) 

class HashtagForm(forms.Form):
	hashtag = forms.CharField(max_length = 50)
	start_time = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local','placeholder':'25/10/2016 14:30'}),input_formats=date_formats)
	end_time = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local','placeholder':'25/10/2018 14:30'}),input_formats=date_formats)

class Top10Form(forms.Form):
	start_time = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local','placeholder':'25/10/2016 14:30'}),input_formats=date_formats)
	end_time = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local','placeholder':'25/10/2018 14:30'}),input_formats=date_formats)

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
    
    # Ut1 = forms.DateTimeField(required=False,widget=SelectDateWidget(years=("2016","2017")))
    # Ut1m = forms.TimeField(widget=forms.TimeInput(format='%H:%M'),required=False)
    # Ut2 = forms.DateTimeField(required=False,widget=SelectDateWidget(years=("2016","2017")))
    # Ut2m = forms.TimeField(widget=forms.TimeInput(format='%H:%M'),required=False)

    Ut1 = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local','placeholder':'25/10/2016 14:30'}),input_formats=date_formats,required=False)
    Ut2 = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local','placeholder':'25/10/2018 14:30'}),input_formats=date_formats,required=False)

    TRelationShip = forms.ChoiceField(choices=[(x,x) for x in [None,"TWEETED"]],required=False)
    TDestination = forms.ModelChoiceField(queryset=Tweet.objects.all(),required=False)

    # Tt1 = forms.DateTimeField(required=False,widget=SelectDateWidget(years=("2016","2017")))
    # Tt1m = forms.TimeField(widget=forms.TimeInput(format='%H:%M'),required=False)

    # Tt2 = forms.DateTimeField(required=False,widget=SelectDateWidget(years=("2016","2017")))
    # Tt2m = forms.TimeField(widget=forms.TimeInput(format='%H:%M'),required=False)
    Tt1 = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local','placeholder':'25/10/2016 14:30'}),input_formats=date_formats,required=False)
    Tt2 = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local','placeholder':'25/10/2018 14:30'}),input_formats=date_formats,required=False)
class EvaluateForm(forms.Form):
    Return_Variables  = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}))
    Query_Name = forms.CharField(widget=forms.TextInput(attrs={'class' : 'myfieldclass'}))

class UploadFileForm(forms.Form):
    name = forms.CharField(max_length = 50)
    file = forms.FileField(required=False)

class CreateCustomMetricForm(forms.Form):
    name = forms.CharField(max_length=50)
    query = forms.ModelChoiceField(queryset=Query.objects.all())
    post_processing_function = forms.ModelChoiceField(queryset=PostProcFunc.objects.all())

class CustomMetricForm(forms.Form):
    metric = forms.ModelChoiceField(queryset=CustomMetric.objects.all())