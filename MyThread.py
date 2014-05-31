import threading
from app import receive

class MyThread(threading.Thread):
    def __init__(self,connection):
        threading.Thread.__init__(self)
        self.connection = connection
 
    def run(self):
        receive(self.connection)