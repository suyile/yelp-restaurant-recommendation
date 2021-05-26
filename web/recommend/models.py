from __future__ import unicode_literals

from django.db import models

# Create your models here.

class  Ratings(models.Model):
    user=models.CharField(max_length=200)
    rating = models.CharField(max_length=200)
    store_id = models.CharField(max_length=200, default='0', primary_key=True)


class Stores(models.Model):
    city=models.CharField(max_length=200,default='city')
    season=models.CharField(max_length=200, default='season')
    store_id=models.CharField(max_length=200,default='0',primary_key=True)
    store_name = models.CharField(max_length=200,default='store')