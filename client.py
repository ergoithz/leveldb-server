#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import logging
import __builtin__

import msgpack

logger = logging.getLogger(__name__)

class TimeoutException(Exception):
    pass

class ExpiredIteratorException(Exception):
    pass

class LevelDB(object):
    class RecvSwitchType(object):
        def __init__(self):
            self.codes = {
                "\0": self.serialized,
                "\1": self.exception,
                "\2": self.iterable,
                }

        def __call__(self, socket, database):
            data_type, data = socket.recv_multipart()
            return self.codes.get(data_type, self.default)(socket, database, data)

        def serialized(self, socket, database, data):
            return msgpack.loads(data)

        def exception(self, socket, database, data):
            exc_type, exc_args = msgpack.loads(data)
            for scope in (globals(), __builtin__.__dict__):
                if exc_type in scope:
                    raise scope[exc_type](*exc_args)
            raise Exception(*exc_args)

        def iterable(self, socket, database, data):
            try:
                data = msgpack.dumps(((msgpack.loads(data),), {}))
                while True:
                    socket.send_multipart((database, "iter_next", data))
                    yield self(socket, database)
            except StopIteration:
                pass

        def default(self, socket, database, data):
            logger.error("Could not parse server message %r" % data)

    recv = staticmethod(RecvSwitchType())

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

    def command(self, cmd, args, kwargs):
        sargs = msgpack.dumps((args, kwargs))
        self.socket.send_multipart((self.database, cmd, sargs))
        return self.recv(self.socket, self.database)

    def get(self, *args, **kwargs):
        return self.command("get", args, kwargs)

    def put(self, *args, **kwargs):
        return self.command("put", args, kwargs)

    def delete(self, *args, **kwargs):
        return self.command("delete", args, kwargs)

    def range_iter(self, *args, **kwargs):
        return self.command("range_iter", args, kwargs)

    def close(self):
        self.socket.close()
        self.context.term()
