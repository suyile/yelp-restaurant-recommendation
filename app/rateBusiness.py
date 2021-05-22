#!/usr/bin/env python

import sys
from os import remove
from os.path import dirname, join, isfile
from time import time

def city_to_state(city):
    return {
        'Edinburgh': 'EDH',
        'Karlsruhe': 'BW',
        'Montreal': 'QC',
        'Waterloo': 'ON',
        'Pittsburgh': 'PA',
        'Charlotte': 'NC',
        'Urbana-Champaign': 'IL',
        'Phoenix': 'AZ',
        'Las Vegas': 'NV',
        'Madison': 'WI',
    }[city]

def top_stores(season, city):
    state = city_to_state(city)
    f = open(topStoresFile, 'r')
    for line in f:
        state_temp = line.strip().split('::')[0]
        season_temp = line.strip().split('::')[1]
        if (season_temp == season) and (state_temp == state):
            topStores = line.strip().split('::')[2]
    f.close()
    return topStores

parentDir = dirname(dirname(__file__))
topStoresFile = join(parentDir, "topStores.txt")
ratingsFile = join(parentDir, "personalRatings.txt")

if isfile(ratingsFile):
    r = raw_input("Looks like you've already rated the movies. Overwrite ratings (y/N)? ")
    if r and r[0].lower() == "y":
        remove(ratingsFile)
    else:
        sys.exit()

promp_1 = "Please select the season: spring, summer, autumn, winter"
print promp_1

seasonStr = raw_input().strip().lower()

promp_2 = '''Please select the city: Edinburgh, Karlsruhe, Montreal, Waterloo, Pittsburgh, Charlotte, Urbana-Champaign, Phoenix, Las Vegas, Madison'''

print promp_2

cityStr = raw_input().strip()

print "The season you chose is: " + seasonStr
print "The city you chose is: " + cityStr
topStores = top_stores(seasonStr, cityStr)

prompt = "Please rate the following stores (1-5 (best), or 0 if not been there): "
print prompt

now = int(time())
n = 0

f = open(ratingsFile, 'w')
for line in topStores.split("|"):
    ls = line.strip().split(",")
    valid = False
    while not valid:
        rStr = raw_input(ls[1] + ": ")
        r = int(rStr) if rStr.isdigit() else -1
        if r < 0 or r > 5:
            print prompt
        else:
            valid = True
            if r > 0:
                f.write("0::%s::%d::%d\n" % (ls[0], r, now))
                n += 1
    if n > 10: break
f.close()

if n == 0:
    print "No rating provided!"