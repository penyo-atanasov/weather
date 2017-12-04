import SocketServer
import socket
import socket
import sys
from time import sleep
from time import sleep
import time
import urllib2
import json
from test.test_itertools import TestExamples
from datetime import datetime
import calendar

conf_file_path = "./iata.txt"
test_data_path = "./test.csv"
actual_values_path = "./actual.csv"
data_feed_interval_duration = 30 * 60




host = '127.0.0.1'
port = 9999 

if __name__ == '__main__':
    if len(sys.argv) > 1:
        conf_file_path = sys.argv[1]
        test_data_path = sys.argv[2]
        actual_values_path = sys.argv[3]
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)
    while True:
        print('\nListening for a client at', host , port)
        conn, addr = s.accept()
        print('\nConnected by', addr)
        start = time.time()
        try:
            test_examples = []
            actual_values = []
            print('\nReading file...\n')
            with open(conf_file_path) as fr:
                for line in fr.readlines():
                    items = line.strip().split(",")
                    try:
                        m = urllib2.urlopen(items[-1]).read()
                        time.sleep(1)  # do not DDOS their servers
                        jsonObj = json.loads(m)
                        print jsonObj
                        obs = jsonObj["observations"]["data"]
                        latest = obs[0]
                        example = []
                        example.append(items[3])  # lat
                        example.append(items[4])  # lon
                        example.append(items[5])  # height
                        example.append(latest["dewpt"])
                        
                        example.append(latest["rain_trace"])
                        d = datetime.utcnow()
                        unixtime = calendar.timegm(d.utctimetuple()) * 1000
                        example.append(unixtime)
                        example.append(latest["delta_t"])
                        example.append(latest["wind_spd_kmh"])
                        actual = []
                        actual.append(latest["apparent_t"])
                        actual.append(latest["press"])
                        actual.append(latest["rel_hum"])
                        # remove the last observation because it is going to be used for prediction.
                        jsonObj["observations"]["data"] = obs[1:]
                        res = json.dumps(jsonObj)
                        conn.send(res + '\n')
                        test_examples.append(",".join([str(x) for x in example]))
                        actual_values.append(",".join([str(x) for x in actual]))
                    finally:
                        pass
            with open(test_data_path, "w") as fw:
                for e in test_examples:
                    if not e.contains("None"):
                        fw.write(e + "\n")
            with open(actual_values_path, "w") as fw:
                for e in actual_values:
                    if not e.contains("None"):
                        fw.write(e + "\n")
                    
        except socket.error:
            print ('Error Occured.\nClient disconnected.\n')
         
        elapsed = time.time() - start
        sleep(data_feed_interval_duration - elapsed)
    conn.close()
