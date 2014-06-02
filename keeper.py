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


keeperId = 0
ports = [0,1,2]
serverMax = 3

vectorClock = [0,0,0]

#the socket to other servers
socketsList = {}

#the list to do the load balancing
loadList = [0]*serverMax

#connect status
disconnect = False


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
        except:
            address =('127.0.0.1',port+5000)
            client.connect(address)
            client.send(json.dumps({'role':'keeper'}))
            #print client.recv(1024)
            client.send(json.dumps(sendData))
        
        data = json.loads(client.recv(1024))
        vectorClock[port] = int(data['clock'])
        loadList[port] = int(data['clientNum'])
        return
    except:
        vectorClock[port] = -1
        return



#keep working
def keeperWork():
    global ports
    global serverMax

    initialConnect()

    while True:
        time.sleep(0.5)
        #print vectorClock
        thread_arr = []
        for port in ports:
            thread_arr.append((MyThread2(port)))

        for i in range(serverMax):
            thread_arr[i].start()

        for i in range(serverMax): 
            thread_arr[i].join()
        sendClock()
        


#send vectorClock
def sendClock():
    global vectorClock
    #global ports
    sendData = {'action':'setClock','vectorClock': vectorClock}
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


#the function to do receive connect from clients
@socketio.on('keeper_server', namespace='/test')
def returnPort():
    minData = min(loadList)
    port = loadList.index(minData)
    emit('server_port', {'pos': port})


if __name__ == '__main__':
    MyThread1().start()
    socketio.run(app,host='0.0.0.0',port=8000+keeperId)