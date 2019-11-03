
import zmq
import zmq.asyncio
import json
import sys
import asyncio
import socket
import numpy
import ast
import ast2json
import pandas as pd
import datetime as dt
from contextlib import closing

__version__ = '0.1.4'


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
        self.ctx = zmq.asyncio.Context()
        self.sock = self.ctx.socket(zmq.REP)
        self.sock.bind(self.endpoint)

    async def listen(self):
        print('NB Agent Gateway (nba_gateway) version %s listening on port %s'
              % (__version__, self.port))
        self.keep_running = True
        while self.keep_running:
            try:
                msg = await self.sock.recv()
                msg = json.loads(msg)
                if (msg['cmd'] == 'stop'):               # stop
                    print("Stopping NBAgateway")
                    self.stop_server()
                elif (msg['cmd'] == 'parse'):            # parse
                    try:  # do not use ast2json.str2json!
                        tree = ast2json.ast2json(ast.parse(msg['code']))
                    except:
                        tree = 'invalid-syntax'
                    s = json.dumps(tree)
                    self.sock.send_string(s)
                elif (msg['cmd'] == 'put_val'):          # put_val
                    var = msg['var']
                    val = msg['val']
                    module = sys.modules["__main__"]
                    setattr(module, var, val)
                    globals()[var] = val   # not sure why both are needed. They are.
                    self.sock.send_string('"OK"')
                elif (msg['cmd'] == 'get_val'):          # get_val
                    var = msg['var']
                    module = sys.modules["__main__"]
                    val = getattr(module, var, "UNKNOWN_VAR")
                    self.sock.send_string(json.dumps(self.numpy2py(val)))
                else:
                    self.sock.send_string("UNKNOWN_CMD")
            except Exception as e:
                print('NBAgateway could not respond. Exception: = %s' % (e,))

    def numpy2py(self, val):
        if isinstance(val, numpy.int64):
            return int(val)
        elif callable(val):
            return("CALLABLE")
        elif (isinstance(val, pd.Timestamp) or
              isinstance(val, dt.datetime) or
              isinstance(val, dt.date)):
            return({'nba-timepoint': str(val)})
        elif isinstance(val, list):
            return [self.numpy2py(x) for x in val]
        elif isinstance(val, numpy.ndarray):
            return self.numpy2py(list(val))
        elif isinstance(val, pd.DataFrame):
            return self.numpy2py(val.to_dict('records'))
        elif isinstance(val, dict):
            for k, v in val.items():
                val[k] = self.numpy2py(val[k])
            return val
        else:
            return val

    async def start_listening(self):
        task = asyncio.create_task(self.listen())
        await task

    def start_server(self):
        loop = asyncio.get_running_loop()
        loop.create_task(self.start_listening())

    def stop_server(self):
        print('Stopping server.')
        self.keep_running = False
