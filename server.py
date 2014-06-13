from gevent import monkey
monkey.patch_all()

import time
import sys
import socket
import json
import threading
import copy
#from socketIO_client import BaseNamespace,SocketIO_client
from threading import Thread
from flask import Flask, render_template, session, request, jsonify, request, current_app, redirect, url_for
from flask.ext.socketio import SocketIO, emit, join_room, leave_room
from Heap import BinHeap
from multiprocessing.dummy import Pool as ThreadPool

app = Flask(__name__)
app.debug = False
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

#set socket timeout


#content
nbString = ""
currCursor = 0
lastCursor = 0

version_num = 0

#lease num
leaseNum = None
lastLease = None

#the socket to other servers
socketsList = {}

#the vector clock in server
serverMax = 3
clock = [0]*serverMax

#the server's id
serverId = 0

#threads list
threadnum = 0
threads = {}

#a map to keep all log
logs = {}

#a map to indicate if the log should be sent
logMap = {}
logPrio = {}
otherLog = {}
sendThreshold = 0

#the log list
logList = BinHeap()

#client num
currClient = 0

#the strings list for roll back
stringList = {}

#the counter for waiting commit
waitCounter = 0

#the map contains the commited logs
logCommited = {}
lastCheck = None

#the thread class
class MyThread1(threading.Thread):
    def __init__(self,connection,sid):
        threading.Thread.__init__(self)
        self.connection = connection
        self.sid = sid
 
    def run(self):
        serverConn(self.connection,self.sid)

class MyThread2(threading.Thread):
    def __init__(self,connection):
        threading.Thread.__init__(self)
        self.connection = connection
 
    def run(self):
        keeperConn(self.connection)

#check the lease
def checkLease():
    global lastLease
    global leaseNum
    while True:
        time.sleep(3)
        if lastLease == None:
            continue
        if lastLease == leaseNum:
            leaveClients()
            sys.exit()
        else:
            lastLease = leaseNum

#let all clients go to other servers
def leaveClients():
    global socketio
    socketio.emit('stop')

#send the head of queue to other clients if deliverable
def sendQueue():
    #with app.app_context():
    global logList
    global socketio
    while (not logList.isEmpty()) and (logList.isDeliverable()) :
        #if deliverable, send it to all 
        topLog = logList.peek()
        prior = logList.getPriority()
        updateText(topLog)
        #otherApp = SocketIO(current_app)
        socketio.emit('my change',{'changedString':topLog['changedString'],'startCursor':topLog['startCursor'],'endCursor':topLog['endCursor'],
                                   'vClock':topLog['vClock'],'version_num':1}, namespace='/test')
        logCommited[json.dumps({'clock':topLog['vClock'],'id':topLog['id']})] = {'log':topLog,'priority':prior}
        logList.remove()
        print "logMap is: "
        print logMap
        if topLog['id'] == serverId:
            del logMap[json.dumps({'clock':topLog['vClock'],'id':topLog['id']})]

#ask other servers to know if the logs can be commit
def askCommit(mid):
    broadcast_server('askCommit',mid)

#combine the logs from disconnect mode
def combineLog(logList):
    pass

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
    global logList
    global sendThreshold

    #open a socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    address = ('127.0.0.1',serverId+5000)
    on = 1
    ret = sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,on)
    if ret<0:
        print "failed to reuse"
    sock.bind(address)
    sock.listen(50)
    print "bind OK!"
    while True:
        try:
            connection,address2 = sock.accept()
            print "new thread!"
            #connection.settimeout(0.5)
            recvdata = json.loads(connection.recv(1024))
            print recvdata
            if str(recvdata['role']) == 'server':
                print socketsList
                if socketsList.has_key(int(recvdata['id'])):
                    print "already has"
                    #continue
                else:
                    succ = addnewSocket(int(recvdata['id']))
                    print "try to connect to the new port"
                    print recvdata['id']
                    if succ == False:
                        continue
                MyThread1(connection,int(recvdata['id'])).start()

            elif str(recvdata['role']) == 'keeper':
                MyThread2(connection).start()

            #Thread(target=receive(connection)).start()

                #connection.sendall(json.dumps(data))
                #print "close good!"
                #connection.close()

            #receive the acknowledgement

        except TypeError,msg:
            print "TypeError"
            print msg

        except:
            print sys.exc_info()[0]
            print "failed to open thread!"
            break
                
    sock.close()
    del sock

#connection to a server
def serverConn(connection,sid):
    global sendThreshold
    global lastCheck
    #print "server receive begin!"
    #print serverId
    print "add threshold"
    sendThreshold += 1
    connId = sid
    while True:
        try:
            recvdata = connection.recv(1024)
            #check if the connection is eligible
            if checkSocket(sid):
                pass
            else:
                connection.close()
                sendThreshold -= 1
                updateCommit(connId)
                return

            if recvdata == None:
                #print "empty recv!"
                continue
            buf = json.loads(recvdata)

            #print "get buf!"
            #print serverId

            print "action is " + buf['action']

            #send the string to other server
            if str(buf['action']) == "requestString":
                #add a socket according to the new server
                data = {'text':nbString,'vclock':clock}
                print json.dumps(data)
                connection.sendall(json.dumps(data))

            #add the log to self queue and send back the acknowledgement
            elif str(buf['action']) == "server_log":

                logList.add({'log':buf['log']})

                locPrio = logList.getPriority()

                #newClkPort = clkPort(buf['log']['vClock'],buf['log']['id'])
                data = {'action':"ack",'vClock':buf['log']['vClock'],'id':buf['log']['id'],'priority':locPrio,'receiver':serverId}

                #update clock
                update_clock(buf['log']['vClock'])

                try:
                    socketsList[buf['log']['id']].sendall(json.dumps(data))

                except:
                    succ = reConn(buf['log']['id'],2)
                    if succ:
                        try:
                            socketsList[buf['log']['id']].sendall(json.dumps(data))
                        except:
                            continue
                    else:
                        #askCommit(json.dumps({'clock':buf['vClock'],'id':buf['id']}))
                        continue

            elif str(buf['action']) == "ack":
                #print 'ack'
                logMap[json.dumps({'clock':buf['vClock'],'id':buf['id']})][int(buf['receiver'])] = True

                if logPrio[json.dumps({'clock':buf['vClock'],'id':buf['id']})] < buf['priority']:
                    logPrio[json.dumps({'clock':buf['vClock'],'id':buf['id']})] = buf['priority']

                #if the num is enough
                if len(logMap[json.dumps({'clock':buf['vClock'],'id':buf['id']})]) >= sendThreshold:
                    logList.setDeliverable(json.dumps({'clock':buf['vClock'],'id':buf['id']}))
                    locPrio = logPrio[json.dumps({'clock':buf['vClock'],'id':buf['id']})]
                    logList.updatePriority(buf['vClock'],buf['id'],locPrio)
                    
                    #del logMap[json.dumps({'clock':buf['vClock'],'id':buf['id']})]
                    del logPrio[json.dumps({'clock':buf['vClock'],'id':buf['id']})]
                    broadcast_server("commit",{'vClock':buf['vClock'],'id':buf['id'],'priority':locPrio})
                    sendQueue()

            #receive the commit command from the coordinator
            elif str(buf['action']) == "commit":
                print 'commit'
                logList.updatePriority(buf['log']['vClock'],buf['log']['id'],buf['log']['priority'])
                logList.setDeliverable(json.dumps({'clock':buf['log']['vClock'],'id':buf['log']['id']}))
                sendQueue()
                print "send OK!"

            #other asks for the information of this 
            elif str(buf['action']) == "askCommit":
                #find it in the logcommit or loglist
                mid = buf['log']
                succ = False
                #if no this log
                if logList.ifDeliverable(mid) == None or findCommit(mid) == None:
                    data = {'action':"Abort",'id':mid}
                    connection.sendall(json.dumps(data))
                #receive commit
                elif logList.ifDeliverable(mid):
                    prior = logList.findPriority(mid)
                    data = {'action':"Accept",'id':mid,'priority':prior}
                    connection.sendall(json.dumps(data)) 
                elif findCommit(mid) != None:
                    prior = findCommit(mid)
                    data = {'action':"Accept",'id':mid,'priority':prior}
                    connection.sendall(json.dumps(data))
                else:
                    continue

            elif str(buf['action']) == "Abort":
                if not logList.isEmpty():
                    topLog = logList.peek()
                    if json.dumps({'clock':topLog['vClock'],'id':topLog['id']}) == buf['id']:
                        logList.remove()
                        lastCheck = None

            elif str(buf['action']) == "Accept":
                if not logList.isEmpty():
                    topLog = logList.peek()
                    if json.dumps({'clock':topLog['vClock'],'id':topLog['id']}) == buf['id']:
                        logList.updatePriority(topLog['vClock'],topLog['id'],buf['priority'])
                        logList.setDeliverable(buf['id'])
                        logList.remove()
                        lastCheck = None
                        sendQueue()

        except:
            print "connection close!"
            print sys.exc_info()[0]
            connection.close()
            sendThreshold -= 1
            updateCommit(connId)
            try:
                #delete the socket to this connection
                del socketsList[sid]
            except:
                pass
            return

#send commit as the lower threshold
def updateCommit(sid):
    global sendThreshold
    for x in logMap:
        #some msg can be committed
        if len(logMap[x]) >= sendThreshold:
            #find if the ack from the disconnected one has been received
            try:
                succ = logMap[x][sid]
            except KeyError:
                buf = json.loads(x)
                locPrio = logPrio[x]
                logList.updatePriority(buf['clock'],buf['id'],locPrio)
                logList.setDeliverable(x)
                
                del logMap[x]
                del logPrio[x]
                broadcast_server("commit",{'vClock':buf['clock'],'id':buf['id'],'priority':locPrio})
                sendQueue()

#find if a log is in the commit list
def findCommit(mid):
    try:
        prior = logCommited[mid]['priority']
        return prior
    except:
        return None

#connection to a keeper
def keeperConn(connection):
    global clock
    global leaseNum
    while True:
        #print "threshold is:"
        #print sendThreshold
        try:
            recvdata = connection.recv(1024)
            if leaseNum == None:
                leaseNum = 0
            else:
                leaseNum += 1
            if recvdata == None:
                print "empty recv!"
                continue
            buf = json.loads(recvdata)
            #print "keeper sends:"
            #print buf
            if str(buf['action']) == "askClock":
                #print clock
                view = socketsList.keys()
                view.append(serverId)
                connection.sendall(json.dumps({'clock':clock[serverId],'clientNum':currClient,'myview':view}))
            
            elif str(buf['action']) == "setClock":
                #check if self in the view
                currView = buf['myview']
                if not (serverId in currView):
                    leaveClients()
                    sys.exit()

                retclock = buf['vectorClock']
                for i in range(len(clock)):
                    clock[i] = retclock[i]
                #print clock
        except:
            print "keeper disconnect"
            print sys.exc_info()[0]
            connection.close()
            return

#reconnect to specific port
def reConn(port,num):
    for x in xrange(0,num):
        try:
            time.sleep(0.1)
            #build a client
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            client.settimeout(5)
            address = ('127.0.0.1',port+5000)
            client.connect(address)
            client.send(json.dumps({'role':'server','id':serverId}))

            #add the client socket to our list
            socketsList[port] = client
            return True
                
        except socket.error,msg:
            continue
    return False
        

#add a new socket to the list
def addnewSocket(newId):
    try:
        #build a client
        succ = reConn(newId,2)
        return succ

    except socket.error,msg:
        return False

def updateText(log):
    global nbString
    global logList

    startCursor = log['startCursor']
    endCursor = log['endCursor']
    changedString = log['changedString']

    if endCursor < startCursor :
        if startCursor <= len(nbString):
            nbString = nbString[0:endCursor] + nbString[startCursor:]
        else:
            nbString = nbString[0:endCursor]
    else:
        if endCursor <= len(nbString) + len(changedString):
            nbString = nbString[0:startCursor] + changedString + nbString[startCursor:]
        else:
            nbString = nbString[0:startCursor] + changedString


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
    tempclock = [-1]*serverMax
    changed = False
    for x in xrange(0,serverMax):
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
                client.sendall(json.dumps({'role':'server','id':serverId}))
                #add the client socket to our list
                socketsList[x] = client

                #request string and get the newest one
                sendData = {'action':"requestString"}
                client.send(json.dumps(sendData))
                recvData = json.loads(client.recv(1024))
                print recvData['vclock']
                print clock
                if clock_compare(tempclock,recvData['vclock']) == False:
                    print 'get here'
                    tempclock = recvData['vclock']
                    tempString = recvData['text']
                    changed = True
                
            except socket.error,msg:
                continue

    if changed == True:
        update_clock(tempclock)
        nbString = tempString
        print "get "+nbString


#update the clock
def update_clock(other):
    global clock
    for i in xrange(0,len(clock)):
        if clock[i] < other[i]:
            clock[i] = other[i]

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
    sendData = {'action':action,'log':log}
    print socketsList
    keycomb = socketsList.keys()
    for i in keycomb:
        try:
            socketsList[i].send(json.dumps(sendData))
        except socket.error,msg:
            succ = reConn(i,2)
            if succ:
                socketsList[i].send(json.dumps(sendData))
            else:     
                print "delete socket!"
                del socketsList[i]
        continue;

#the thread to see if the top log has stayed too long
def checkLate():
    global lastCheck
    time.sleep(3)
    if not logList.isEmpty():
        if lastCheck == None:
            lastCheck = logList.peek()
        elif lastCheck == logList.peek():
            askCommit(json.dumps({'clock':lastCheck['vClock'],'id':lastCheck['id']}))

#find if the sid is in the socketsList
def checkSocket(sid):
    try:
        trySock = socketsList[sid]
        return True
    except:
        return False

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('connect', namespace='/test')
def textRequest():
    print "try to get text!"
    #emit('server_connect',{'port' : serverId})

@socketio.on('disconnect', namespace='/test')
def leave():
    global currClient
    currClient -= 1
    print "one connection leaves!"

@socketio.on('my connect', namespace='/test')
def test_message(message):
    global nbString
    global currClient
    #join_room(serverId)
    currClient += 1
    print "get new string!"
    disconnLogs = message['data']
    if len(disconnLogs) != 0:
        combineLog(disconnLogs)
    emit('my connect', {'nbString':nbString})


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
    clock[serverId] += 1
    refClock = copy.deepcopy(clock)
    newlog = {'changedString':message['changedString'],'startCursor': message['startCursor'],
              'endCursor': message['endCursor'],'vClock': refClock,'deliverable':False,'id':serverId,'version_num':1}

    #if only one server
    if sendThreshold == 0:
        updateText(newlog)
        socketio.emit('my change',newlog, namespace='/test')
        return

    print "1"
    logList.add({'log':newlog})

    print "2"
    locPrio = logList.getPriority()

    print "3"
    logPrio[json.dumps({'clock':clock,'id':serverId})] = locPrio
    
    #socketio.emit('my change',{'changedString':message['changedString'],'startCursor':message['startCursor'],'endCursor':message['endCursor'],'version_num':1})
    #to other servers
    print "4"
    logMap[json.dumps({'clock':clock,'id':serverId})] = {}
    broadcast_server("server_log",newlog)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit(0)
    serverId = int(sys.argv[1])
    thread = Thread(target=listenServer)
    thread.start()
    thread2 = Thread(target=checkLate)
    thread2.start()
    thread3 = Thread(target=checkLease)
    thread3.start()
    connectInitial()
    socketio.run(app,host='0.0.0.0',port=serverId+10000)






'''
def commit(message):
    session['receive_count1'] = session.get('receive_count1', 0) + 1
    emit('my change', {'changedString': message['changedString'],
                       'lastCursor': message['lastCursor'],
                       'currCursor': message['currCursor'],
                       'lClock': clock}, broadcast=True)
'''