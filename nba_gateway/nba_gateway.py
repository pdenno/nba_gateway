
import socket
from contextlib import closing    
import time
import threading
import zmq
import json
from io import StringIO
import sys
import asyncio
import nest_asyncio
nest_asyncio.apply()

class NBAgateway(object):
    def __init__(self,port):
        self.port = port
        self.endpoint = 'tcp://*:' + str(port)
        self.sock = None
        self.keep_running = False
        self.ctx = zmq.Context()

    async def listen(self):
        print('NBAgateway listening on port', self.port)
        print('context is %s' % (self.ctx,))
        self.sock = self.ctx.socket(zmq.REP)
        self.sock.bind(self.endpoint)
        self.keep_running = True
        print('socket is %s' % (self.sock,))
        while self.keep_running:
            try:
                await asyncio.sleep(4)
                try:
                    msg = self.sock.recv(flags=zmq.NOBLOCK, track=True)
                    print('Past the recv()!!!!! msg = %s' % (msg,))
                    msg = json.loads(msg)
                except zmq.ZMQError as e:
                    print(' exception is %s' % (e,))
                    msg = False
                finally:
                    print('In finally, msg = %s' % (msg,))
                print('msg or False: %s' % (msg,))
                if msg:
                    if (msg['cmd'] == 'stop'):               # stop 
                        print("Stopping NBAgateway")
                        self.stop_server()
                    elif (msg['cmd'] == 'put_val'):          # put_val
                        var = msg['var']
                        val = msg['val']
                        print('var = %s, val = %s' % (var, val))
                        globals()[var] = val
                        self.sock.send_string('"OK"')
                    elif (msg['cmd'] == 'get_val'):          # get_val
                        var = msg['var']
                        print('get_val: %s' % (var,))
                        if (var in (globals)()):
                            print('found in globals: %s' % (globals()[var],))
                            self.sock.send_string(json.dumps(globals()[var]))
                        else:
                            print('did not find %s' % (var,))
                            self.sock.send_string('"UNKNOWN_VAR"')
                    else:
                        self.sock.send_string('"UNKNOWN_CMD"')
            except Exception as e:
                print('NBAgateway could not respond. Stopping. Exception: = %s' % (e,))
                self.stop_server()
#            finally:
#                self.sock.close()

    async def start_listening(self):
        task = asyncio.create_task(self.listen())
        await task

    def start_server(self):
        loop = asyncio.get_running_loop()
        loop.create_task(self.start_listening())

    def stop_server(self):
        print('Stopping server.')
        self.keep_running = False
 #       self.sock.close()
