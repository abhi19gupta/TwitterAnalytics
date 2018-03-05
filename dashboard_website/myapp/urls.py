from django.conf.urls import url
from myapp import views

urlpatterns = [
    url(r'^$', views.home, name='home'),
    url(r'^hashtags/$', views.hashtags, name='hashtags'),
    url(r'^hashtags/hashtag_usage_getter/$', views.hashtag_usage_getter, name='hashtag_usage_getter'),
    url(r'^hashtags/hashtag_top10_getter/$', views.hashtag_top10_getter, name='hashtag_top10_getter'),
    url(r'^hashtags/hashtag_sentiment_getter/$', views.hashtag_sentiment_getter, name='hashtag_sentiment_getter'),
    url(r"^create_metric/",views.query_creator,name="create_metric"),
    url(r"^create_query_handler/$",views.create_query_handler,name="create_query_handler"),
    url(r"^create_postprocessing_handler/$",views.create_postprocessing_handler,name="create_postprocessing_handler"),
    url(r"^create_custom_metric_handler/$", views.create_custom_metric_handler, name="create_custom_metric_handler"),
    url(r"^view_custom_metric_handler/$", views.view_custom_metric_handler, name="view_custom_metric_handler")
]