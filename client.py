#!/usr/bin/env python
# -*- coding: UTF-8 -*-

class LevelDB(object):

    def __init__(self, host, database, timeout=3, gevent=False):
        self.host = host
        self.database = database
        self.timeout = timeout

        if gevent:
            import zmq.green as zmq
        else:
            import zmq

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.XREQ)
        self.socket.connect(self.host)

    def send(self, cmd, *args):
        params = [self.database, cmd]
        params.extend(str(i) for i in args)
        self.socket.send_multipart(params)

    parsers = {
        "\0": lambda x: None, # Means key not found
        "\1": lambda x: None, # Means stop iteration
        "\xFE": lambda x: x[1],
        "\xFF": lambda x: x[1:],
        }
    def parse(self, data):
        return self.parsers[data[0]](data)

    def recv(self):
        return self.parse(self.socket.recv_multipart())

    def recv_multi(self):
        data = self.socket.recv_multipart()
        while data[0] != "\1":
            yield self.parse(data)
            data = self.socket.recv_multipart()

    def get(self, key):
        self.send("get", key)
        return self.recv()

    def put(self, key, value):
        self.send("put", key, value)
        return self.recv()

    def delete(self, key):
        self.send("delete", key)
        return self.recv()

    def range(self, start=None, end=None):
        """
        Range return key/value items
        """
        self.send("range", start, end)
        return self.recv_multi()

    def close(self):
        self.socket.close()
        self.context.term()
