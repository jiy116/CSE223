from gevent import monkey
monkey.patch_all()

import time
import heapq
import random
import sys
import app
from threading import Thread
from flask import Flask, render_template, session, request,jsonify,request
from flask.ext.socketio import SocketIO, emit, join_room, leave_room

def run(port):
	app.runApp(port)

#get the number of servers we want
if len(sys.argv) < 2:
	sys.exit(0)

num = int(sys.argv[1])
print num
run(num+10000)

#for x in range(0,num):
#	print x+10000
#	Thread(target=run(x+10000)).start()