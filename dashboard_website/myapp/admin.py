from django.contrib import admin
from myapp.models import *

# Register your models here.
admin.site.register(Tweet)
admin.site.register(User)
admin.site.register(Relation)
admin.site.register(Query)
admin.site.register(PostProcFunc)
admin.site.register(CustomMetric)
admin.site.register(QueryInput)
admin.site.register(QueryOutput)
admin.site.register(AlertSpecification)
