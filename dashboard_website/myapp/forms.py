from django import forms

class HashtagForm(forms.Form):
	hashtag = forms.CharField(max_length = 50)
	start_time = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local'}),input_formats=['%Y-%m-%dT%H:%M'])
	end_time = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local'}),input_formats=['%Y-%m-%dT%H:%M'])

class Top10Form(forms.Form):
	start_time = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local'}),input_formats=['%Y-%m-%dT%H:%M'])
	end_time = forms.DateTimeField(widget=forms.TextInput(attrs={'type': 'datetime-local'}),input_formats=['%Y-%m-%dT%H:%M'])