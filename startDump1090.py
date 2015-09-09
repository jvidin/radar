__author__ = 'jo'

import os

try:
    os.system('./dump1090 --interactive --net')
except Exception as e:
    print (e)