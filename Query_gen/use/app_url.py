from django.conf.urls import url,include
from . import views
from django.contrib.auth import views as auth_views

urlpatterns = [
    url(r"query/$",views.query,name = 'query'),

]
