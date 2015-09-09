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
            time.sleep(1)
    except:
        pass


def persist_data(data):

    try:
        print(data)
        table = db['radar']
        for each in data:
            if each['flight'] is not '' and (each['lat'] > 0):
                print(each)
                table.insert(dict(each))

    except Exception as e:
        print(e)



if __name__ == '__main__':
    get_data()

