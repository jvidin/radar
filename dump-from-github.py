import datetime
import json
import requests
import time

get_data = lambda: json.loads(requests.get('http://192.168.1.3:8080/data.json').content)
data_diff = lambda new, old: [x for x in new if x not in old]
make_date = lambda: str(datetime.datetime.now())

old_data = []

while True:
        try:
                new_data = get_data()
                diffs = data_diff(new_data, old_data)
                old_data = new_data
                if len(diffs) > 0:
                        diff_lines = [','.join([str(y).strip() for y in [make_date()] + x.values()]) for x in diffs]
                        for line in diff_lines:
                                print line
        except:
                pass
        time.sleep(0.5)