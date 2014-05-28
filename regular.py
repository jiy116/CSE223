from gevent import monkey
monkey.patch_all()

import time
import heapq
from threading import Thread
from flask import Flask, render_template, session, request,jsonify,request
from flask.ext.socketio import SocketIO, emit, join_room, leave_room

app = Flask(__name__)
app.debug = False
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

#the most clients can connected to the server, dynamic change
clientLimit = 10

#content
nbstring = ""
clock = 0
nbpq = []
serverComb = []
connClient = 0

#the id assigned to clients
clientId = 0


@app.route('/')
def index():
    return render_template('index.html')

#first connected by others
@socketio.on('connect', namespace='/test')
def test_message(message):

    #if the connection is OK
    if connClient < clientLimit:
        connClient = connClient + 1
        join_room('port')
        emit('my connect', {'admit':True,'port':name,'currString':nbstring})
    else:
        emit('my connect', {'admit':False})

#get information from clients
@socketio.on('my log', namespace='/test')
def log_send(message):
    global nbstring
    global clock

    #update the clock
    clock = clock +1

    #push the log into the priority queue
    nbpq.push()
    
    emit('my change', {'changedString': message['changedString'],
                       'lastCursor': message['lastCursor'],
                       'currCursor': message['currCursor'],
                       'lClock': clock}, broadcast=True)

#communicate with other servers
@socketio.on('join')
def on_join(data):
    emit('join respond',{'result':True})

def run(port):
    socketio.run(app,host='0.0.0.0',port=port)

#open the app's function
#def runApp(port):
if __name__ == '__main__':
    #print port
    #SocketIO.set
    #open a server with specific port
    app.run(host='0.0.0.0',port=20000)