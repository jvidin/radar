__author__ = 'jo'

import time

import requests
import dataset
import os
import itertools
import operator
from operator import itemgetter


db = dataset.connect('sqlite:///dbradar.db')

def startdump():
    try:
        os.system('nohup ./dump1090 --interactive --net')
    except Exception as e:
        print (e)

def get_data():
    try:
        stream = []
        while True:
            json_data = requests.get('http://192.168.1.4:8080/data.json')
            data = json_data.json()
            #persist_data(data)
            for each in data:
                stream.append(each)
                if len(stream) > 100:
                    DuplicateRemover(stream)
                    stream = []
                    get_data()

            time.sleep(1)
    except:
        pass

def DuplicateRemover(stream):
    ## http://stackoverflow.com/questions/11092511/python-list-of-unique-dictionaries
    #cleanStream = {v['flight']['lon']['lat']: v for v in stream}.values()
    ##solution found for de-deduplicate , dont fylly understand but seems to wrk
    ##http://stackoverflow.com/questions/7090758/python-remove-duplicate-dictionaries-from-a-list/7091256#7091256

    cleanStream = []
    #print(stream)
    getvals = operator.itemgetter('flight','lat','lon')
    stream.sort(key=getvals)
    for k, g in itertools.groupby(stream,getvals):
        cleanStream.append(g.next())
    stream[:] = cleanStream
    print(cleanStream)
    persist_data(cleanStream)

def persist_data(data):
    try:
        #print(data)
        cnt = 0
        with db as tx:
            for each in data:
                if each['flight'] is not '' and (each['lat'] > 0):
                    cnt+=1
                    #print(each)
                    tx['radar'].insert(dict(each))
        print(str(cnt)+' values inserted')
    except Exception as e:
        print(e)



if __name__ == '__main__':
    get_data()
