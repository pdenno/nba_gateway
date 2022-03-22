# nba_gateway - A small library for communicating with a running Jupyter notebook

This small library runs inside the Jupyter kernel process connecting through ZMQ to service requests
to get and set the values of variables, and evaluate Python ast2json library calls to parse Python 
expressions. It is started using a Python magic command in the notebook. 
