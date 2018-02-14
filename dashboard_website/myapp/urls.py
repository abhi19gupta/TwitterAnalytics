from django.conf.urls import url
from myapp import views

urlpatterns = [
    url(r'^$', views.home, name='home'),
    url(r'^hashtags/$', views.hashtags, name='hashtags'),
    url(r'^hashtags/hashtag_usage_getter/$', views.hashtag_usage_getter, name='hashtag_usage_getter'),
    url(r'^hashtags/hashtag_top10_getter/$', views.hashtag_top10_getter, name='hashtag_top10_getter'),
    url(r'^hashtags/hashtag_sentiment_getter/$', views.hashtag_sentiment_getter, name='hashtag_sentiment_getter'),
]