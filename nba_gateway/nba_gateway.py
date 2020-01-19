
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
from pysat.examples.rc2 import RC2
from pysat.formula import WCNF

__version__ = '0.1.8'


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
        self.module = sys.modules["__main__"]

    async def listen(self):
        print('NB Agent Gateway (nba_gateway) version %s listening on port %s'
              % (__version__, self.port))
        self.keep_running = True
        while self.keep_running:
            try:
                msg = await self.sock.recv()
                msg = json.loads(msg)
                if (msg['cmd'] == 'stop'):               # ---stop--
                    print("Stopping NBAgateway")
                    self.stop_server()
                elif (msg['cmd'] == 'parse'):            # ---parse--
                    try:  # do not use ast2json.str2json!
                        tree = ast2json.ast2json(ast.parse(msg['code']))
                    except Exception as e:
                        tree = {'request': 'parse',
                                'data': msg,
                                'status': 'invalid-syntax',
                                'error-msg': str(e)}
                    self.sock.send_string(json.dumps({'request': 'parse',
                                                      'data': msg,
                                                      'status': 'OK',
                                                      'tree': tree}))
                elif (msg['cmd'] == 'put_val'):          # ---put_val--
                    var = msg['var']
                    val = msg['val']
                    setattr(self.module, var, val)
                    globals()[var] = val   # Both are needed. Strange.
                    self.sock.send_string(json.dumps({'request': 'put_val',
                                                      'data': msg,
                                                      'status': 'OK'}))
                elif (msg['cmd'] == 'get_val'):          # ---get_val--
                    if ('max' in msg.keys()):
                        mt = msg['max']
                    else:
                        mt = False
                    val = getattr(self.module, msg['var'], 'UNKNOWN_VAR')
                    if (type(val) == str) and (val == "UNKNOWN_VAR"):  # can't == a DF to a string!
                        self.sock.send_string(json.dumps({'request': 'get_val',
                                                          'data': msg,
                                                          'status': 'UNKNOWN_VAR'}))
                    else:
                        self.sock.send_string(json.dumps({'request': 'get_val',
                                                          'data': msg,
                                                          'status': 'OK',
                                                          'result': self.numpy2py(val, max_table=mt)}))
                elif (msg['cmd'] == 'var_type'):         # ---var_type--
                    val = getattr(self.module, msg['var'], "UNKNOWN_VAR")
                    if (type(val) == str) and (val == "UNKNOWN_VAR"):  # can't == a DF to a string!
                        self.sock.send_string(json.dumps({'request': 'var_type',
                                                          'data': msg,
                                                          'status': 'UNKNOWN_VAR'}))
                    else:
                        self.sock.send_string(json.dumps({'request': 'var_type',
                                                          'data': msg,
                                                          'status': 'OK',
                                                          'result': self.type_data(val)}))
                elif (msg['cmd'] == 'MAX-SAT'):          # --- MAX-SAT problem
                    s = msg['problem']
                    try:
                        wcnf = WCNF(from_string=s)
                        RC2(wcnf).compute()
                        result = []
                        cnt = 0
                        with RC2(wcnf) as rc2:
                            for m in rc2.enumerate():  # rc2.cost huh?
                                if (cnt < 10):
                                    cnt += 1
                                    result.append({'model': m, 'cost': rc2.cost})
                    except:
                        self.sock.send_string(json.dumps({'request': 'MAX-SAT',
                                                          'data': msg,
                                                          'status': 'RC2_FAILED'}))
                        continue
                    self.sock.send_string(json.dumps({'request': 'MAX-SAT',
                                                      'data': msg,
                                                      'status': 'OK',
                                                      'result': result}))
                else:
                    self.sock.send_string(json.dumps({'request': msg['cmd'],
                                                      'data': msg,
                                                      'status': 'UNKNOWN_CMD'}))
            except Exception as e:
                print('NBAgateway could not respond. Exception: %s\n msg = %s' % (e, msg))
                self.sock.send_string(json.dumps({'request': msg['cmd'],
                                                  'data': msg,
                                                  'status': 'NBA_GATEWAY_FAILED',
                                                  'error-msg': str(e)}))
                continue

    def type_data(self, val):
        try:
            result = {'type': type(val).__name__}
            if isinstance(val, pd.DataFrame):
                result['shape'] = val.shape
                result['columns'] = val.columns
            elif isinstance(val, list):
                result['size'] = len(val)
            return result
        except Exception as e:
            print('type_data, Exception: %s' % (e,))

    def numpy2py(self, val, max_table=False):
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
            if max_table and (val.shape[0] > max_table):
                val = pd.DataFrame(val[0:max_table])
            return self.numpy2py(val.to_dict('records'))
        elif isinstance(val, dict):
            for k, v in val.items():
                val[k] = self.numpy2py(val[k])
            return val
        else:
            try:
                if numpy.isnan(val):  # Amazing! isnan doesn't except all types!
                    return({'nba-numpy-nan': 'nan'})
                else:
                    return val
            except:
                return({'unhandled-type': str(val)})


    async def start_listening(self):
        task = asyncio.create_task(self.listen())
        await task

    def start_server(self):
        loop = asyncio.get_running_loop()
        loop.create_task(self.start_listening())

    def stop_server(self):
        print('Stopping server.')
        self.keep_running = False
