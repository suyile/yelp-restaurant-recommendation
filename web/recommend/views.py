from django.shortcuts import render
import time

from .models import Stores


#from .models import Ratings
# Create your views here.
#from django.http import HttpResponse


def index(request):
    return render(request, 'recommend/index.html')


#rate view/
def rate(request):
    season = request.GET.get('season', None)
    city = request.GET.get('city', None)
    if season is None:
        raise "User didn't select the season,please select one!"

    if city is None:
        raise "User didn't select the city, please select one!"

    store_list = Stores.objects.filter(season=season, city=city)[:10]

    context = {
        'store_list': store_list,
        'season': season,
        'city': city,
    }
    return render(request, 'recommend/ratinglist.html', context)

def result(request):

    url=request.get_full_path()

    from task_recommend import StoreRecommender
    stores = StoreRecommender().delay(url)

    while not stores.ready():
        time.sleep(1)

    stores_recommend=stores.get()

    print "Stores recommended for you:"
    for i in xrange(len(stores_recommend)):
        print ("%2d: %s" % (i + 1, stores_recommend[i].split('::')[0])).encode('ascii', 'ignore')

    context = {
        'stores_recommend': stores_recommend,
    }

    return render(request, 'recommend/resultlist.html', context)