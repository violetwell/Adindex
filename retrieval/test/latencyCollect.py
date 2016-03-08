import socket
import sys
import time
import re

result = {}
file_object = open(sys.argv[1])
sum = 0
count = 0
#file_object = ['"LATENCY":"-1"','"LATENCY":"88"','"LATENCY":"8"','"LATENCY":"888"','"LATENCY":"8888"','"LATENCY":"88888"']
for line in file_object:
    try:
        key = ""
        latArr = re.findall('get \d in (\d*?) ms',line)
        if len(latArr):
            latency = int(latArr[0])
            sum += latency
            count += 1
            if 0 <= latency < 100:
                i = latency/10
                key = str(i*10) + "-" + str(i*10 + 10)
            else:
                key = "100+"
                
            if not result.has_key(key):
                result[key] = 1
            else:
                result[key] += 1
    except Exception, e:
        print "Exception",e,"latArr[0]",latArr[0]

print "average: ", str(float(sum)/count), ",count: ", str(count)    
for j in range(0,9):
    key = str(j*10) + "-" + str(j*10 + 10)
    if result.has_key(key):
        print key + ": " + str(result[key]) + ", percent: " + str(float(result[key])/count)
     