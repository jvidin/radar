__author__ = 'jo'

import time

import requests
import dataset
import os

db = dataset.connect('sqlite:///dbradar.db')

def startdump():
    try:
        os.system('nohup ./dump1090 --interactive --net')
    except Exception as e:
        print (e)

def get_data():
    try:
        while True:
            json_data = requests.get('http://192.168.1.4:8080/data.json')
            data = json_data.json()
            persist_data(data)
            #query_db(data)
            time.sleep(1)
    except:
        pass


def query_db(data):

    try:
        for each in data:
            if each['flight'] is not '':
                print(each)
                flight = each['flight']
                lat = each['lat']
                lon = each['lon']
                altitude = each['altitude']
                sql = ('select count(*) as value from radar where flight="{}" and lat={} and lon={} and altitude={}').format(flight,lat,lon,altitude)
                in_db = db.query(sql)
                for result in in_db:
                    print(result)
                    value = int(result['value'])
                    print(value)
                    if value > 0:
                        pass
                    else:
                        print('persist')
                        persist_data_each(each)
    except Exception as e:
        print(e)



def persist_data(data):
    try:
        table = db['radar']
        for each in data:
            print(each)
            if each['flight'] is not '' and (each['lat'] > 0):
                print('inserting > ' + each)
                table.insert(dict(each))

    except Exception as e:
        print(e)

def persist_data_each(each):
    try:
        table = db['radar']
        print('inserting > ' + each)
        table.insert(dict(each))

    except Exception as e:
        print(e)



if __name__ == '__main__':
    get_data()

