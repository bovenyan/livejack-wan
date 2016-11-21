#!/usr/bin/python
import socket
import time

if __name__ == '__main__':
    port = 6789

    city_ip = {}
    mapFile = open('map', 'r')
    data = mapFile.readline()
    while data != "":
        city,ip = data.split(',')
        city_ip[city]=ip
        data = mapFile.readline()
    mapFile.close()

    i = 1
    streamers = {}
    # init client socket
    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while True:
        trace = open(str(i),'r')
        data = trace.readline()
        livingStreamer = set()
        while data != "":
            print data
            seq, cType, cPos, cTarget, liveId = data.split(',')
            if cPos not in city_ip or cTarget not in city_ip:
                continue
            if cType == "s":
                livingStreamer.add(liveId)
                if liveId in streamers:
                    continue
                streamers[liveId] = city_ip[cPos]
            # connect to server
            clientSocket.connect((city_ip[cPos], port))
            # send HTTP request
            print cType, cTarget, liveId
            clientSocket.send(cType + "," + city_ip[cTarget] + "," + liveId)
            data = trace.readline()
        for streamer in streamers:
            if streamer not in livingStreamer:
                clientSocket.connect((streamers[streamer], port))
                print "d,None," + streamer
                clientSocket.send("d,None," + streamer)
        time.sleep(60)
        i += 1