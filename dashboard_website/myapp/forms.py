from django import forms
from myapp.models import *
from django.forms.extras.widgets import SelectDateWidget

class HashtagForm(forms.Form):
	hashtag = forms.CharField(max_length = 50)
	start_time = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local'}),input_formats=['%Y-%m-%dT%H:%M'],required=False)
	end_time = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local'}),input_formats=['%Y-%m-%dT%H:%M'])

class Top10Form(forms.Form):
	start_time = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local'}),input_formats=['%Y-%m-%dT%H:%M'])
	end_time = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local'}),input_formats=['%Y-%m-%dT%H:%M'])

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

    Ut1 = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local'}),input_formats=['%Y-%m-%dT%H:%M'],required=False)
    Ut2 = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local'}),input_formats=['%Y-%m-%dT%H:%M'],required=False)

    TRelationShip = forms.ChoiceField(choices=[(x,x) for x in [None,"TWEETED"]],required=False)
    TDestination = forms.ModelChoiceField(queryset=Tweet.objects.all(),required=False)

    # Tt1 = forms.DateTimeField(required=False,widget=SelectDateWidget(years=("2016","2017")))
    # Tt1m = forms.TimeField(widget=forms.TimeInput(format='%H:%M'),required=False)

    # Tt2 = forms.DateTimeField(required=False,widget=SelectDateWidget(years=("2016","2017")))
    # Tt2m = forms.TimeField(widget=forms.TimeInput(format='%H:%M'),required=False)
    Tt1 = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local'}),input_formats=['%Y-%m-%dT%H:%M'],required=False)
    Tt2 = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local'}),input_formats=['%Y-%m-%dT%H:%M'],required=False)
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