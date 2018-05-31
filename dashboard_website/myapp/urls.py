from django.conf.urls import url
from myapp import views

urlpatterns = [
	url(r'^$', views.home, name='home'),
	url(r'^hashtags/$', views.hashtags, name='hashtags'),
	url(r'^hashtags/hashtag_usage_getter/$', views.hashtag_usage_getter, name='hashtag_usage_getter'),
	url(r'^hashtags/hashtag_top10_getter/$', views.hashtag_top10_getter, name='hashtag_top10_getter'),
	url(r'^hashtags/hashtag_sentiment_getter/$', views.hashtag_sentiment_getter, name='hashtag_sentiment_getter'),

	url(r"^create_metric/",views.query_creator,name="create_metric"),
	url(r"^create_neo4j_query_handler/$",views.create_neo4j_query_handler,name="create_neo4j_query_handler"),
	url(r"^create_mongo_query_handler/$",views.create_mongo_query_handler,name="create_mongo_query_handler"),
	url(r"^create_postprocessing_handler/$",views.create_postprocessing_handler,name="create_postprocessing_handler"),
	url(r"^create_custom_metric_handler/$", views.create_custom_metric_handler, name="create_custom_metric_handler"),

	url(r"^view_custom_metric_handler/$", views.view_custom_metric_handler, name="view_custom_metric_handler"),
	url(r"^view_query_handler/$", views.view_query_handler, name="view_query_handler"),
	url(r"^delete_query_handler/$", views.delete_query_handler, name="delete_query_handler"),
	url(r"^view_post_proc_handler/$", views.view_post_proc_handler, name="view_post_proc_handler"),
	url(r"^delete_post_proc_handler/$", views.delete_post_proc_handler, name="delete_post_proc_handler"),

	url(r"^alerts/$", views.alerts, name="alerts"),
	url(r"^alerts/alerts_view", views.alerts_view, name="alerts_view"),
	url(r"^alerts/alerts_delete", views.alerts_delete, name="alerts_delete"),
	url(r"^alerts/alerts_activate", views.alerts_activate, name="alerts_activate"),
	url(r"^alerts/alerts_deactivate", views.alerts_deactivate, name="alerts_deactivate"),
	url(r"^alerts/alerts_tweets", views.alerts_tweets, name="alerts_tweets"),
	url(r"^alerts/alerts_dismiss", views.alerts_dismiss, name="alerts_dismiss"),
	url(r"^alerts/alerts_create_handler", views.alerts_create_handler, name="alerts_create_handler"),

	url(r"^create_query/$", views.query_creator, name="create_query"),

	url(r"^create_query/create_neo4j_query_handler/$",views.create_neo4j_query_handler,name="create_neo4j_query_handler"),
	url(r"^create_query/create_mongo_query_handler/$",views.create_mongo_query_handler,name="create_mongo_query_handler"),
	url(r"^create_query/create_postprocessing_handler/$",views.create_postprocessing_handler,name="create_postprocessing_handler"),
	url(r"^create_query/delete_query_handler/(?P<query>[0-9a-zA-Z_-]+)$",views.delete_query_handler,name="delete_query_handler"),

	url(r"^create_query/create_dag_handler", views.create_dag_handler, name ="create_dag_handler"),
	url(r"^create_query/view_dag_handler/(?P<dag_name>[0-9a-zA-Z_-]+)", views.view_dag_handler, name ="view_dag_handler"),
	url(r"^create_query/delete_dag_handler/(?P<dag_name>[0-9a-zA-Z_-]+)", views.delete_dag_handler, name ="delete_dag_handler"),

]
