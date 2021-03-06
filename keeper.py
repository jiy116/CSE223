from gevent import monkey
monkey.patch_all()

import time
import sys
import socket
import json
import threading
import heapq
#from socketIO_client import BaseNamespace,SocketIO_client
from threading import Thread
from flask import Flask, render_template, session, request, jsonify, request, current_app, redirect, url_for
from flask.ext.socketio import SocketIO, emit, join_room, leave_room
from Heap import BinHeap
from multiprocessing.dummy import Pool as ThreadPool


#the thread class
class MyThread1(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
 
    def run(self):
        keeperWork()


class MyThread2(threading.Thread):
    def __init__(self,port):
        threading.Thread.__init__(self)
        self.port = port
 
    def run(self):
        dialServer(self.port)



app = Flask(__name__)
app.debug = False
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

validserver = 0
keeperId = 0
ports = [0,1,2]
serverMax = 3

vectorClock = [0,0,0]

#the socket to other servers
socketsList = {}

#the list to do the load balancing
loadList = [9999999]*serverMax

#connect status
disconnect = False

#every one's view
viewdic = {}

#current view
viewlist = []

currview = []

#dialServer
def dialServer(port):
    global vectorClock
    try: 

        client = socketsList[port]
        sendData = {'action':'askClock'}
        client.settimeout(0.5)
        #redail
        try:
            client.send(json.dumps(sendData))
            data = json.loads(client.recv(1024))
        except:

            newclient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            newclient.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            address =('127.0.0.1',port+5000)
            newclient.connect(address)
            newclient.send(json.dumps({'role':'keeper'}))
            #newclient.recv(1024)
            socketsList[port] = newclient
            sendData = {'action':'askClock'}
            newclient.send(json.dumps(sendData))
            data = json.loads(newclient.recv(1024))

        vectorClock[port] = int(data['clock'])
        loadList[port] = int(data['clientNum'])
        viewdic[port] = data['myview']

        return
    except:
        vectorClock[port] = -1
        loadList[port] = 999999999
        return



#keep working
def keeperWork():
    global ports
    global serverMax
    global viewdic
    global validserver
    initialConnect()

    while True:
        time.sleep(1.5)
        viewdic = {}
        #print vectorClock
        thread_arr = []
        for port in ports:
            thread_arr.append((MyThread2(port)))

        for i in range(serverMax):
            thread_arr[i].start()

        for i in range(serverMax): 
            thread_arr[i].join()


        getView()
        sendClock()
        validserver = count_alive()

        
        
def getView():
    global dicview
    global currview
    currview = []

    for myview in viewdic:
        ret = findview(viewdic[myview])
        if len(ret)>len(currview):
            currview = ret


#find one's view
def findview(set):
    global viewdic
    ret = range(serverMax)
    for node in set:
        ret = merge(viewdic[node],ret)
    return ret


#merge two set2
def merge(set1,set2):
    dic = {}
    ret = []
    for node in set1:
        dic[node] = 1
    for node in set2:
        if node in dic:
            ret.append(node)
    #print ret
    return ret


#count how many server alive
def count_alive():
    global validserver
    global verctorClock

    validserver = 0
    for i in vectorClock:
        if i != -1:
            validserver += 1
    return validserver


#send vectorClock
def sendClock():
    global vectorClock
    #global ports
    sendData = {'action':'setClock','vectorClock': vectorClock,'myview': currview}
    for port in ports:
        try:
            socketsList[port].sendall(json.dumps(sendData))
        except:
            pass


def initialConnect():

    global socketsList
    global ports
    HOST = '127.0.0.1'    # The remote host

    for port in ports:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        address =('127.0.0.1',port+5000)
        try:
            client.settimeout(5)
            client.connect(address)
            client.send(json.dumps({'role':'keeper'}))
            client.recv(1024)

        except:
            pass
        socketsList[port] = client

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('connect',namespace='/test')
def newClient():
    print "new client!"


#the function to do receive connect from clients
@socketio.on('keeper_server', namespace='/test')
def returnPort():
    global validserver
    if validserver == 0:
        emit('server_port',{'pos':-1})
        return
    minData = min(loadList)
    port = loadList.index(minData)
    emit('server_port', {'pos': port})


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit(0)
    keeperId = int(sys.argv[1])
    MyThread1().start()

    socketio.run(app,host='0.0.0.0',port=8000+keeperId)
