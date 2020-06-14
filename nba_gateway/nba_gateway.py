
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
import logging
import os
if os.path.exists('nba_gateway.log'):
    os.remove('nba_gateway.log')

logging.basicConfig(filename='nba_gateway.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(message)s')

__version__ = '0.1.12'


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
        logging.info('nba_gateway started at port {0}'.format(self.port))

    def handle_parse(self, msg):
        try:  # do not use ast2json.str2json!
            response = {'request': 'parse', 'data': msg}
            # logging.info('handle_parse {0}.'.format(msg)) # Too much output
            response['tree'] = ast2json.ast2json(ast.parse(msg['code']))
            response['status'] = 'OK'
        except Exception:
            response['status'] = 'INVALID_SYNTAX'
        self.sock.send_string(json.dumps(response))

    def handle_eval(self, msg):
        try:
            mt = False
            if ('max' in msg.keys()):
                mt = msg['max']
            response = {'request': 'eval_expr', 'data': msg}
            logging.info('handle_eval {0}.'.format(msg))
            val = eval(msg['code'], self.module.__dict__)
            response['result'] = self.numpy2py(val, max_collection=mt)
            response['status'] = 'OK'
        except Exception as e:
            response['status'] = 'EVAL_FAILED'
            response['error-msg'] = str(e)
            logging.info('Error in handle_eval: {0}'.format(e))
        self.sock.send_string(json.dumps(response))

    def handle_put_val(self, msg):
        logging.info('handle_put_val {0}.'.format(msg))
        var = msg['var']
        val = msg['val']
        setattr(self.module, var, val)
        globals()[var] = val   # Both are needed. Strange.
        self.sock.send_string(json.dumps({'request': 'put_val',
                                          'data': msg,
                                          'status': 'OK'}))

    def handle_get_val(self, msg):
        logging.info('handle_get_val {0}.'.format(msg))
        mt = False
        if ('max' in msg.keys()):
            mt = msg['max']
        val = getattr(self.module, msg['var'], 'UNKNOWN_VAR')
        if (type(val) == str) and (val == "UNKNOWN_VAR"):  # can't == a DF to a string!
            self.sock.send_string(json.dumps({'request': 'get_val',
                                              'data': msg,
                                              'status': 'UNKNOWN_VAR'}))
        else:
            self.sock.send_string(json.dumps({'request': 'get_val',
                                              'data': msg,
                                              'status': 'OK',
                                              'result': self.numpy2py(val, max_collection=mt)}))

    def handle_var_type(self, msg):
        logging.info('handle_var_type {0}.'.format(msg))
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

    def handle_maxsat(self, msg):
        logging.info('handle_maxsat.')
        s = msg['problem']
        max_cnt = int(msg['max_cnt']) if 'max_cnt' in msg else 11
        response = {'request': 'MAX-SAT', 'data': msg, 'status': 'OK'}
        try:
            wcnf = WCNF(from_string=s)
            RC2(wcnf).compute()
            result = []
            cnt = 0
            with RC2(wcnf) as rc2:
                for m in rc2.enumerate():  # rc2.cost huh?
                    if (cnt < max_cnt):
                        cnt += 1
                        result.append({'model': m, 'cost': rc2.cost})
                    else:
                        break
            response['result'] = result
        except Exception as e:
            response['status'] = 'RC2_FAILED'
            response['error-msg'] = str(e)
        self.sock.send_string(json.dumps(response))

    async def listen(self):
        print('NB Agent Gateway (nba_gateway) version %s listening on port %s'
              % (__version__, self.port))
        logging.info('NB Agent Gateway (nba_gateway) version {0} listening on port {1}'.
                     format(__version__, self.port))
        self.keep_running = True
        while self.keep_running:
            try:
                msg = await self.sock.recv()
                msg = json.loads(msg)
                if (msg['request'] == 'stop'):
                    print("Stopping NBAgateway")
                    self.stop_server()
                elif (msg['request'] == 'parse'):
                    self.handle_parse(msg)
                elif (msg['request'] == 'eval_expr'):
                    self.handle_eval(msg)
                elif (msg['request'] == 'put_val'):
                    self.handle_put_val(msg)
                elif (msg['request'] == 'get_val'):
                    self.handle_get_val(msg)
                elif (msg['request'] == 'var_type'):
                    self.handle_var_type(msg)
                elif (msg['request'] == 'MAX-SAT'):
                    self.handle_maxsat(msg)
                else:
                    logging.info('Unknown request: {0}.'.format(msg))
                    self.sock.send_string(json.dumps({'request': msg['request'],
                                                      'data': msg,
                                                      'status': 'UNKNOWN_CMD'}))
            except Exception as e:
                logging.error('NBAgateway could not respond. Exception: {0}. msg = {1}.'.format(e, msg))
                print('NBAgateway could not respond. Exception: %s\n msg = %s' % (e, msg))
                self.sock.send_string(json.dumps({'request': msg['request'],
                                                  'data': msg,
                                                  'status': 'NBA_GATEWAY_FAILED',
                                                  'error-msg': str(e)}))
                continue

    def type_data(self, val):
        try:
            result = {'type': type(val).__name__}
            if isinstance(val, pd.DataFrame):
                result['shape'] = val.shape
                result['columns'] = list(val.columns)
            elif isinstance(val, list):
                result['size'] = len(val)
            return result
        except Exception as e:
            logging.info('Error in type_data: {0}'.format(e))
            print('type_data, Exception: %s' % (e,))

    def numpy2py(self, val, max_collection=False):
        try:
            if isinstance(val, (int, float, str, bool)):
                return val
            elif isinstance(val, numpy.int64):
                return int(val)
            elif callable(val):
                return("CALLABLE")
            elif (isinstance(val, pd.Timestamp) or
                  isinstance(val, dt.datetime) or
                  isinstance(val, dt.date)):
                return({'nba-timepoint': str(val)})
            elif isinstance(val, list):
                if max_collection and (len(val) > max_collection):
                    val = val[0:max_collection]
                return [self.numpy2py(x) for x in val]
            elif isinstance(val, numpy.ndarray):
                return self.numpy2py(list(val))
            elif isinstance(val, pd.core.series.Series):
                return self.numpy2py(dict(val))
            elif isinstance(val, pd.DataFrame):
                if max_collection and (val.shape[0] > max_collection):
                    val = pd.DataFrame(val[0:max_collection])
                return self.numpy2py(val.to_dict('records'))
            elif isinstance(val, dict):
                for k, _ in val.items():
                    val[k] = self.numpy2py(val[k])
                return val
            elif numpy.isnan(val):  # Amazing! isnan doesn't except all types!
                return({'nba-numpy-nan': 'nan'})
            else:
                logging.info('numpy2py unhandled type: {0}'.format(type(val).__name__))
                return {'unhandled-type': type(val).__name__}
        except Exception as e:
            logging.info('Error in numpy2py type of val: {0}, error:{1}'.format(type(val).__name__, e))
            return({'unhandled-type': 'exception in numpy2py'})

    async def start_listening(self):
        task = asyncio.create_task(self.listen())
        await task

    def start_server(self):
        loop = asyncio.get_running_loop()
        loop.create_task(self.start_listening())

    def stop_server(self):
        print('Stopping server.')
        self.keep_running = False
