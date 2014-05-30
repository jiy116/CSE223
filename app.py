from gevent import monkey
monkey.patch_all()

import time
import sys
import socket
import json
#from socketIO_client import BaseNamespace,SocketIO_client
from threading import Thread
from flask import Flask, render_template, session, request,jsonify,request,current_app
from flask.ext.socketio import SocketIO, emit, join_room, leave_room
from Heap import BinHeap
from multiprocessing.dummy import Pool as ThreadPool
from clkPort import clkPort

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
serverMax = 1
clock = [0]*3

#the server's id
serverId = 0

#a map to keep all log
logs = {}

#a map to indicate if the log should be sent
logMap = {}
sendThreshold = 0

#the log list
logList = BinHeap()

#send the head of queue to other clients if deliverable
def sendQueue():
    #with app.app_context():
    global logList
    global socketio
    while True:
        time.sleep(1)
        if logList.isEmpty() == False and logList.isDeliverable() == True:
            logList.printheap()
            topLog = logList.peek()
            print topLog
            updateText(topLog)
            #otherApp = SocketIO(current_app)
            socketio.emit('my change',{'changedString':topLog['changedString'],'startCursor':topLog['startCursor'],'endCursor':topLog['endCursor'],'version_num':1},
                          {'room':serverId,'broadcast':True,'namespace':'/test'})
            #logList.remove()

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
    global logMap

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
        try:
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
                connection.close()

            #add the log to self queue and send back the acknowledgement
            elif str(buf['action']) == "server_log":
                logList.add(buf['log'])
                #newClkPort = clkPort(buf['log']['vClock'],buf['log']['id'])
                data = {'action':"ack",'vClock':buf['log']['vClock'],'id':buf['log']['id']}
                print data
                print socketsList
                connection.close()
                newClient, succ = reConn(buf['log']['id'],2)
                if succ == True:
                    socketsList[buf['log']['id']].sendall(json.dumps(data))
                else:
                    continue

                #connection.sendall(json.dumps(data))
                #print "close good!"
                #connection.close()

            #receive the acknowledgement
            elif str(buf['action']) == "ack":
                newClkPort = clkPort(buf['vClock'],buf['id'])
                print logMap
                logMap[json.dumps({'clock':buf['vClock'],'id':buf['id']})] = logMap[json.dumps({'clock':buf['vClock'],'id':buf['id']})]+1
                print logMap
                print logMap[json.dumps({'clock':buf['vClock'],'id':buf['id']})]
                connection.close()
                #if the num is enough
                if logMap[json.dumps({'clock':buf['vClock'],'id':buf['id']})] >= sendThreshold:
                    logList.setDeliverable(json.dumps({'clock':buf['vClock'],'id':buf['id']}))
                    del logList[json.dumps({'clock':buf['vClock'],'id':buf['id']})]
                    broadcast_server("commit",{'vClock':buf['vClock'],'id':buf['id']})

            #receive the commit command from the coordinator
            elif str(buf['action']) == "commit":
                newClkPort = clkPort(buf['log']['vClock'],buf['log']['id'])
                logList.setDeliverable(json.dumps({'clock':buf['vClock'],'id':buf['id']}))
                connection.close()

        except:
            if connection != None:
                connection.close()
                
    sock.close()
    del sock


#reconnect to specific port
def reConn(port,num):
    for x in xrange(0,num):
        try:
            #build a client
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            client.settimeout(5)
            address = ('127.0.0.1',port+5000)
            client.connect(address)

            #add the client socket to our list
            socketsList[port] = client
            return client,True
                
        except socket.error,msg:
            continue
    return None,False
        

#add a new socket to the list
def addnewSocket(newId):
    try:
        #build a client
        client,succ = reConn(newId,2)

        #add the client socket to our list
        socketsList[newId] = client
        return True
                
    except socket.error,msg:
        return False

def updateText(log):
    global nbString
    global logList

    print log
    startCursor = log['startCursor']
    endCursor = log['endCursor']
    changedString = log['changedString']

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

#broadcast some data to all other servers
def broadcast_server(action,log):
    global socketsList
    print action
    print log
    sendData = {'action':action,'log':log}
    print socketsList
    keycomb = socketsList.keys()
    for i in keycomb:
        print i
        try:
            reConn(i,2)
            socketsList[i].send(json.dumps(sendData))
        except socket.error,msg:
            print "delete socket!"
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


@socketio.on('my connect', namespace='/test')
def test_message(message):
    global nbString
    print "go go go!"+nbString
    join_room(serverId)
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

    version_num = version_num + 1
    logs[version_num] = log

    #update the vector clock and save it in the log list
    clock[serverId] = clock[serverId] + 1
    newlog = {'changedString':message['changedString'],'startCursor': message['startCursor'],
              'endCursor': message['endCursor'],'vClock': clock,'deliverable':False,'id':serverId}
    logList.add(newlog)

    #to other servers
    newClkPort = clkPort(clock,serverId)
    logMap[json.dumps({'clock':clock,'id':serverId})] = 0
    broadcast_server("server_log",newlog)


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