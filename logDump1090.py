__author__ = 'jo'

import sys
import datetime
import time
import requests
import os
import itertools
import operator
import logging
from operator import itemgetter
import psycopg2
from psycopg2.extensions import AsIs

conn = psycopg2.connect("dbname='radar' user='postgres' host='192.168.1.3' password='password'")


def init_db(table_name):
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
    stream = []
    while True:
        json_data = requests.get('http://192.168.1.3:8080/data.json')
        data = json_data.json()
        for each in data:
            #print each
            stream.append(each)
            if len(stream) > 100:
                duplicate_remover(stream)
                stream = []
                get_data()

        time.sleep(1)


def insert_into_table(clean_stream):
    #print 'stream .. ', cleanStream
    #print 'stream tuple' , tuple(cleanStream)

    try:
       with conn.cursor() as cur:
            for each in clean_stream:
                keys = each.keys()
                values = [each[key] for key in keys]
                insert_statement = 'insert into radar (%s) values %s'
                cur.execute(insert_statement, (AsIs(','.join(keys)), tuple(values)))
                conn.commit()

            ts = time.time()
            st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            print(str(st) + ' ' + str(len(clean_stream)) + ' values inserted')

    except Exception as e:
        print e


def duplicate_remover(stream):
    #  http://stackoverflow.com/questions/11092511/python-list-of-unique-dictionaries
    # cleanStream = {v['flight']['lon']['lat']: v for v in stream}.values()
    # solution found for de-deduplicate , dont fully understand but seems to wrk
    # http://stackoverflow.com/questions/7090758/python-remove-duplicate-dictionaries-from-a-list/70912567091256
    # TODO Evaluate this gist on future refatoring of the script https://gist.github.com/th0ma5w/10205889

    cleanStream = []
    #print(stream)
    getvals = operator.itemgetter('flight', 'lat', 'lon')
    stream.sort(key=getvals)
    for k, g in itertools.groupby(stream, getvals):
        cleanStream.append(g.next())
    stream[:] = cleanStream
    #print(cleanStream)
    insert_into_table(cleanStream)


def logi_log(message):
    logging.basicConfig(filename='log.txt', level=logging.DEBUG)
    logging.debug(message+'values inserted')


if __name__ == '__main__':
    get_data()
    #initDatabase()

