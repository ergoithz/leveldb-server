#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import logging

logger = logging.getLogger(__name__)

class LevelDB(object):
    class ParserSwitchType(object):
        def __init__(self):
            self.codes = {
                "\0": self.none, # Special meaning: StopIteration
                "\1": self.none,
                "\2": self.key_error,
                "\xFB": self.exception,
                "\xFE": self.string,
                "\xFF": self.string_tuple,
                }

        def __call__(self, data):
            return self.codes.get(data[0], self.default)(data)

        def none(self, data):
            return None

        def key_error(self, data):
            raise KeyError(*data[1:])

        def exception(self, data):
            raise Exception(*data[1:])

        def string(self, data):
            return data[1]

        def string_tuple(self, data):
            return data[1:]

        def default(self, data):
            logger.error("Could not parse server message %r" % data)

    parse = staticmethod(ParserSwitchType())

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
