#keeper

import time
import sys
import socket
import json
import clkPort
#from socketIO_client import BaseNamespace,SocketIO_client
from threading import Thread
from Heap import BinHeap
from multiprocessing.dummy import Pool as ThreadPool


keeperId = 0
ports = [0,1,2]

count = 0

vectorClock = [0,0,0]

#the socket to other servers
socketsList = {}


#dialServer
def dialServer(port):
    global vectorClock
    global count
    try: 
        client = socketsList[port]
        client.settimeout(0.5)

        sendData = {'action':'askClock'}
        
        #redail
        try:
            client.send(json.dumps(sendData))
        except:
            address =('127.0.0.1',port)
            client.connect(address)
            client.send(json.dumps(sendData))
        
        data = client.recv(1024)

        vectorClock[port] = int(data)
        count = count + 1
        return
    except socket.error,msg:
        vectorClock[port] = -1
        count = count + 1
        return


#keep working
def main():
    global count
    global ports
    initialConnect()
    while True:
        print vectorClock
        time.sleep(0.5)
        count = 0
        for port in ports:
            thread = Thread(target=dialServer(port))
            thread.start()
        while count!=3:
            pass
        sendClock()


#send vectorClock
def sendClock():
    global vectorClock
    #global ports

    sendData = {'vectorClock': vectorClock}
    for port in ports:
        try:
            socketsList[port].send(json.dumps(sendData))
        except:
            pass



def initialConnect():

    global socketsList
    global ports
    HOST = '127.0.0.1'    # The remote host


    for port in ports:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        address =('127.0.0.1',port)
        try:
            client.connect(address)
        except:
            pass
        socketsList[port]=client


if __name__ == "__main__":
    main()













































































if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit(0)
    serverId = int(sys.argv[1])
    socketio.run(app,host='0.0.0.0',port=serverId+10000)