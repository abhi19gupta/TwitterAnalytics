from django.contrib import admin
from .models import *

admin.site.register(Tweet)
admin.site.register(User)
admin.site.register(Relation)

# Register your models here.
