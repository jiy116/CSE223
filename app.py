from gevent import monkey
monkey.patch_all()

import time
import sys
import socket
import json
#from socketIO_client import BaseNamespace,SocketIO_client
from threading import Thread
from flask import Flask, render_template, session, request,jsonify,request
from flask.ext.socketio import SocketIO, emit, join_room, leave_room
from Heap import BinHeap

app = Flask(__name__)
app.debug = False
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

#content
nbString = ""
currCursor = 0
lastCursor = 0

version_num = 0

#the socket to other servers
socketsList = {}

#the vector clock in server
clock = [0]*3

#the server's id
serverId = 0

#a map to keep all log
logs = {}

#the log list
logList = BinHeap[]

#send the head of queue to other clients if deliverable
def sendQueue():
    global logList
    while True:
        if logList.isEmpty() == False && logList.isDeliverable() == True:
            topLog = logList.peek()
            emit('my change',{'newLog':topLog})
            logList.remove()

def checkLog():
    global logList
    mylog = logList.peek()
    sendData = {'action':"checkLog",'theLog':mylog}
    for index in range(len(socketsList)):
        try:
            socketsList[index].send(json.dumps(sendData))

        except:
            #delete the socket if it can not be connected
            socketsList[index].close()
            del socketsList[index]
            continue
    return True

def listenServer():
    global nbString

    #open a socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    address = ('127.0.0.1',serverId+5000)
    on = 1
    ret = sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,on)
    if ret<0:
        print "failed to reuse"
    sock.bind(address)
    sock.listen(5)
    print "bind OK!"
    while True:
        connection,address2 = sock.accept()
        buf = json.loads(connection.recv(1024))
        print "action is " + buf['action']

        #send the string to other server
        if str(buf['action']) == "requestString":
            #add a socket according to the new server
            succ = addnewSocket(buf['id'])
            if succ == False:
                continue
            data = {'text':nbString,'vclock':clock}
            print json.dumps(data)
            connection.sendall(json.dumps(data))

        #add the log to self queue
        elif str(buf['action']) == "server_log":
            logList.add(buf['log'])

        #get the request to check the log
        elif str(buf['action']) == "checkLog":

            
        connection.close()
    sock.close()
    del sock

#add a new socket to the list
def addnewSocket(newId):
    newport = newId+5000
    try:
        #build a client
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        client.settimeout(5)
        address = ('127.0.0.1',newport)
        client.connect(address)

        #add the client socket to our list
        socketsList[newId] = client
        return True
                
    except socket.error,msg:
        return False

def updateText(log):
    global nbString
    startCursor = log[1]
    endCursor = log[2]
    changedString = log[0]

    if endCursor < startCursor :
        if startCursor <= len(nbString):
            nbString = nbString[0:endCursor]+nbString[startCursor:]
        else:
            nbString = nbString[0:endCursor]
    else:
        if endCursor <= len(nbString):
            nbString = nbString[0:startCursor] + changedString+nbString[endCursor:]
        else:
            nbString = nbString[0:startCursor] + changedString
    print nbString


def updateLog(changedString,startCursor,endCursor):
    curr_version = version_num+1
    while curr_version in logs:
        log = logs[curr_version]
        
        if log[1] <= startCursor:
            # @update the cursor
            diff = log[1]-log[2]
            endCursor = endCursor + diff
            startCursor = startCursor + diff

        curr_version = curr_version + 1
    return (changedString,startCursor,endCursor)

#get string from other servers
def connectInitial():
    global nbString
    global socketsList
    global clock

    tempString = ""
    tempclock = [0]*3
    changed = False
    for x in xrange(0,3):
        if x == serverId:
            continue
        else:
            newport = x + 5000
            address = ('127.0.0.1',newport)
            print newport
            try:
                #build a client
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                client.settimeout(5)
                client.connect(address)

                #add the client socket to our list
                socketsList[x] = client

                #request string and get the newest one
                sendData = {'action':"requestString",'id':serverId}
                client.send(json.dumps(sendData))
                recvData = json.loads(client.recv(1024))
                if clock_compare(tempclock,recvData['vclock']) == False:
                    tempclock = recvData['vclock']
                    tempString = recvData['text']
                    changed = True
                
            except socket.error,msg:
                continue

    if changed == True:
        clock = tempclock
        nbString = tempString

#compare two vector clocks
def clock_compare(clock1,clock2):
    for i in xrange(0,len(clock1)):
        if clock1[i] < clock2[i]:
            return False
        elif clock1[i] > clock2[i]:
            return True
    return False

#broadcast the log to all other servers
def broadcast_server(log):
    global socketsList
    sendData = {'action':"server_log",'log':log}
    print socketsList
    for i in range(len(socketsList)):
        try:
            socketsList[i].send(json.dumps(sendData))
        except socket.error,msg:
            del socketsList[i]
            continue;


@app.route('/')
def index():
    thread = Thread(target=listenServer)
    thread.start()
    thread2 = Thread(target=sendQueue)
    thread2.start()
    connectInitial()
    return render_template('index.html')

@socketio.on('connect')
def textRequest():
    print "try to get text!"
    emit('server_connect',{'port' : serverId})

@socketio.on('server_connect')
def textGiving(message):
    join_room('server')
    print "send text!"
    emit('initial_text',{'text' : nbString, 'vector_clock' : clock})

@socketio.on('initial_text')
def initialText(message):
    print "initialize text!"
    nbString = message['text']


@socketio.on('my connect', namespace='/test')
def test_message(message):
    global nbString
    print "go go go!"+nbString
    emit('my connect', {'data': message['data'],'nbString':nbString})


#get the log from clients
@socketio.on('my log', namespace='/test')
def log_send(message):
    global version_num
    global nbString
    global logs
    global logList
    global clock
    global serverId

    log = updateLog(message['changedString'],message['startCursor'],message['endCursor'])

    updateText(log)
    version_num = version_num + 1
    logs[version_num] = log

    #update the vector clock and save it in the log list
    clock[serverId] = clock[serverId] + 1
    newlog = {'changedString':message['changedString'],'startCursor': message['startCursor'],
              'endCursor': message['endCursor'],'vClock': clock,'deliverable':False,'id':serverId}
    logList.add(newlog)

    #to other servers
    broadcast_server(newlog)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit(0)
    serverId = int(sys.argv[1])
    socketio.run(app,host='0.0.0.0',port=serverId+10000)






'''
def commit(message):
    session['receive_count1'] = session.get('receive_count1', 0) + 1
    emit('my change', {'changedString': message['changedString'],
                       'lastCursor': message['lastCursor'],
                       'currCursor': message['currCursor'],
                       'lClock': clock}, broadcast=True)
'''