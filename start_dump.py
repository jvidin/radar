__author__ = 'jo'

import os

try:
    os.system('/home/pi/dump1090/dump1090 --interactive --net')
except Exception as e:
    print (e)
