from django.shortcuts import render, redirect
from django.http import JsonResponse
from myapp.forms import HashtagForm,Top10Form

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
	else:
		print(form['hashtag'].errors, form['start_time'].errors, form['end_time'].errors)
	
	data = {"x":[1,2,3,4], "y":[6,2,5,2]}
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
	else:
		print(form['start_time'].errors, form['end_time'].errors)
	
	data = [{"hashtag":"Sports","count":132}, {"hashtag":"Politics","count":95}, {"hashtag":"Health","count":55}, {"hashtag":"Cricket","count":34}]
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
	else:
		print(form['hashtag'].errors, form['start_time'].errors, form['end_time'].errors)
	
	data = {"x":[1,2,3,4], "y":[6,2,5,2]}
	return JsonResponse(data)