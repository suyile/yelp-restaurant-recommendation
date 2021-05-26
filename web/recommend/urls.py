from django.conf.urls import url

from . import views

app_name='recommend'

urlpatterns = [

    url(r'^$', views.index, name='index'),
    #need to change to ..../
    #url(r'^(?P<season>\w+)/(?P<city>\w+)$', views.rate, name='rating'),
    url(r'^search$', views.rate, name='rating'),
    #url(r'^(?P<question_id>[0-9]+)/$', views.detail, name='detail'),

    url(r'^result$', views.result, name='result'),

]


# http://localhost:8000/recommend/spring/vancouver

# http://localhost:8000/recommend/search?season=spring&city=vancouver
# http://localhost:8000/recommend/search