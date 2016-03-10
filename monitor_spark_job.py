#!/usr/bin/python
import json
import urllib2
import sys
from datetime import datetime
spark_master='http://localhost:8090/jobs'
json_response=urllib2.urlopen(spark_master).read()

from pprint import pprint
json_data=json.loads(json_response)
status_count=0
exception_list = dict()

def days_between(d1):
    from datetime import datetime
    d1 = datetime.strptime(d1, "%Y-%m-%dT%H:%M:%S.%fZ")
    d2 = datetime.now()
    return abs((d2 - d1).days)

for result in json_data:
    #print result['startTime']
    if result['status'] == 'ERROR' and days_between( result['startTime'] ) == 0:
        status_count=1
        exception_list[result['jobId']] = result['result']['message']

if status_count == 0:
    print "Jobs OK: No Failed jobs"
    sys.exit(0)
else:
    print "CRITICAL: Jobs Failed", exception_list
    sys.exit(2)
    #pprint(exception_list)
