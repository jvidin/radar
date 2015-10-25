import sys

__author__ = 'jo'

import datetime
import time
import requests
import dataset
import os
import itertools
import operator
import logging
from operator import itemgetter
import psycopg2
from psycopg2.extensions import AsIs

db = dataset.connect('sqlite:///dbradar.db')


 # drop table if exists radar3;
 # CREATE TABLE radar3
 # (
 #   id serial NOT NULL,
 #   squawk text,
 #   flight text,
 #   hex text,
 #   track integer,
 #   lon double precision,
 #   altitude integer,
 #   vert_rate integer,
 #   messages integer,
 #   validposition integer,
 #   validtrack integer,
 #   lat double precision,
 #   seen integer,
 #   speed integer,
 #   geom geography(POINT,4326),
 #   CONSTRAINT radar3_pkey PRIMARY KEY (id)
 # );
 # INSERT INTO radar3 (geom) VALUES (ST_GeographyFromText('POINT(-9.375732 38.742122)') );
conn = psycopg2.connect("dbname='radar' user='postgres' host='192.168.1.4' password='password'")

def initDatabase():
    try:

        cur = conn.cursor()

        cur.execute("""
                drop table if exists radar;
                 CREATE TABLE radar
                 (
                   squawk VARCHAR(10),
                   flight VARCHAR(10),
                   hex VARCHAR(10),
                   track VARCHAR(10),
                   lon VARCHAR(10),
                   altitude VARCHAR(10),
                   vert_rate VARCHAR(10),
                   messages VARCHAR(10),
                   validposition VARCHAR(10),
                   validtrack VARCHAR(10),
                   lat VARCHAR(10),
                   seen VARCHAR(10),
                   speed VARCHAR(10),
                   geom geography(POINT,4326)
                 );""")
        conn.commit()

    except Exception, e:
        print e
        print "I am unable to connect to the database"



def get_data():
    try:
        stream = []
        while True:
            json_data = requests.get('http://192.168.1.4:8080/data.json')
            data = json_data.json()
            for each in data:
                stream.append(each)
                if len(stream) > 10:
                    DuplicateRemover(stream)
                    #insertIntoTable(stream)
                    stream = []
                    get_data()

            time.sleep(1)
    except:
        pass

def insertIntoTable(cleanStream):
    #print 'stream .. ', cleanStream
    #print 'stream tuple' , tuple(cleanStream)

    try:
        cur = conn.cursor()
        for each in cleanStream:
            keys = each.keys()
            values = [each[key] for key in keys]
            insert_statement = 'insert into radar (%s) values %s'
            cur.execute(insert_statement, (AsIs(','.join(keys)), tuple(values)))
            conn.commit()

        # ts = time.time()
        # st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        # print(str(st) + ' ' + str(cnt)+' values inserted')
    except Exception as e:
        print e
        raise

def DuplicateRemover(stream):
    #  http://stackoverflow.com/questions/11092511/python-list-of-unique-dictionaries
    # cleanStream = {v['flight']['lon']['lat']: v for v in stream}.values()
    # solution found for de-deduplicate , dont fylly understand but seems to wrk
    # http://stackoverflow.com/questions/7090758/python-remove-duplicate-dictionaries-from-a-list/70912567091256

    cleanStream = []
    print(stream)
    getvals = operator.itemgetter('flight','lat','lon')
    stream.sort(key=getvals)
    for k, g in itertools.groupby(stream,getvals):
        cleanStream.append(g.next())
    stream[:] = cleanStream
    print(cleanStream)
    #persist_data(cleanStream)
    insertIntoTable(cleanStream)

def persist_data(data):
    try:
        cnt = 0
        with db as tx:
            for each in data:
                if each['flight'] is not '' and (each['lat'] > 0):
                    cnt+=1
                    print(each)
                    tx['radar2'].insert(dict(each))
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        print(str(st) + ' ' + str(cnt)+' values inserted')
        logilog(str(cnt))
    except Exception as e:
        print(e)

def logilog(message):
    logging.basicConfig(filename='log.txt',level=logging.DEBUG)
    logging.debug(message+'values inserted')


if __name__ == '__main__':
    initDatabase()
    get_data()
