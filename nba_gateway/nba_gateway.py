
import zmq
import json
import sys
import asyncio
import socket
from contextlib import closing
    
class NBAgateway():
    def __init__(self):
        # https://stackoverflow.com/questions/1365265/on-localhost-how-do-i-pick-a-free-port-number
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.port = s.getsockname()[1]
        self.endpoint = 'tcp://*:' + str(self.port)
        self.sock = None
        self.keep_running = False
        self.ctx = zmq.Context()
        self.sock = self.ctx.socket(zmq.REP)
        self.sock.bind(self.endpoint)

    async def listen(self):
        print('NB Agent Gateway (nba_gateway) listening on port', self.port)
        self.keep_running = True
        while self.keep_running:
            try:
                await asyncio.sleep(0.5)
                try:
                    msg = self.sock.recv(flags=zmq.NOBLOCK) # await here doesn't work
                    msg = json.loads(msg)
                except zmq.ZMQError as e:
                    msg = False
                if msg:
                    if (msg['cmd'] == 'stop'):               # stop 
                        print("Stopping NBAgateway")
                        self.stop_server()
                    elif (msg['cmd'] == 'put_val'):          # put_val
                        var = msg['var']
                        val = msg['val']
                        module = sys.modules["__main__"]
                        setattr(module, var, val)
                        globals()[var] = val # not sure why both are needed. They are.
                        self.sock.send_string('"OK"')
                    elif (msg['cmd'] == 'get_val'):          # get_val
                        var = msg['var']
                        if (var in (globals)()):
                            self.sock.send_string(json.dumps(globals()[var]))
                        else:
                            self.sock.send_string('"UNKNOWN_VAR"')
                    else:
                        self.sock.send_string('"UNKNOWN_CMD"')
            except Exception as e:
                print('NBAgateway could not respond. Stopping. Exception: = %s' % (e,))
                self.stop_server()

    async def start_listening(self):
        task = asyncio.create_task(self.listen())
        await task

    def start_server(self):
        loop = asyncio.get_running_loop()
        loop.create_task(self.start_listening())

    def stop_server(self):
        print('Stopping server.')
        self.keep_running = False
