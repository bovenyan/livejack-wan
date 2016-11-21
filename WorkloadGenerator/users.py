#!/usr/bin/python
from socket import *
import thread
import subprocess
import time

class Users(object):
    def __init__(self):
        self.viewers = 0
        self.streamers = 0
        self.streaming = {}

    def run(self):
        def sessionThread(self, connectionSocket, addr):
            print "Thread started!"
            message = connectionSocket.recv(1024)
            # parse client request
            ctype, ctarget, liveId = message.split(",")
            print ctype, ctarget, liveId
            thread.start_new_thread(clientThread, (self, ctype, ctarget, liveId))
            # Close client socket
            connectionSocket.close()
            print "Connection thread stopped!"

        def clientThread(self, clientType, clientTarget, liveId):
            if clientType == "v":
                viewers = self.viewers
                self.viewers += 1
                log = open("viewer" + str(viewers), 'a+')
                command = subprocess.Popen(['rtmpdump', '-r', clientTarget + "live/bunny_" + liveId], stdout=log,
                                           stderr=log)
                print "Creating viewer", viewers
                time.sleep(60)
                command.kill()
                log.close()
                print "Viewer"
            elif clientType == "s":
                streamers = self.streamers
                self.streamers += 1
                log = open("streamer" + str(streamers), 'a+')
                command = subprocess.Popen(
                    ['ffmpeg', '-re', '-i', 'Bunny_360', '-c:v', 'mpeg4', '-c:a', 'aac', '-strict', '-2', '-f', 'flv', 'rtmp://',
                     clientTarget + "/live/bunny_" + liveId], stdout=log, stderr=log)
                self.streaming[liveId] = (command, log)
                print "Created stream", liveId
            elif clientTarget == "d":
                command, log = self.streaming[liveId]
                command.kill()
                log.close()
                del self.streaming[liveId]
                print "Killed stream", liveId

        serverSocket = socket(AF_INET, SOCK_STREAM)
        # Prepare a server socket
        serverSocket.bind(('', 6789))
        serverSocket.listen(3)

        while True:
            # Establish the connection
            print 'Ready to serve...'
            (connectionSocket, addr) = serverSocket.accept()
            # start a new connection socket thread
            thread.start_new_thread(sessionThread, (self, connectionSocket, addr,))

        serverSocket.close()

if __name__ == '__main__':
    Users().run()