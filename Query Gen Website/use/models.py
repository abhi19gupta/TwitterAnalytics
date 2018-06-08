from django.db import models
from django.utils.deconstruct import deconstructible
from django.contrib.auth.models import User
from datetime import datetime
# Create your models here.


class Tweet(models.Model):
    tname = models.CharField(max_length=200)
    hashtag = models.CharField(max_length=200)
    retweet_of = models.CharField(max_length=200)
    reply_of = models.CharField(max_length=200)
    quoted = models.CharField(max_length=200)
    has_mention = models.CharField(max_length=200)
    def __str__(self):
        return (self.tname)
class User(models.Model):
    uname = models.CharField(max_length=200)
    userid = models.CharField(max_length=200)

    def __str__(self):
        return (self.uname)

class Relation(models.Model):
    source = models.CharField(max_length=200)
    relation = models.CharField(max_length=200)
    destn = models.CharField(max_length=200)
    bt = models.CharField(max_length=200)
    et = models.CharField(max_length=200)

