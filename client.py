#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import collections
import logging
import sys
import __builtin__

import msgpack


class TimeoutException(Exception):
    pass


class ExpiredIteratorException(Exception):
    pass


class CommandProtocol(object):
    '''
    LevelDB-server protocol implementation.
    '''
    def __init__(self, size, green, host, database, timeout=-1, socket_type=None):
        if green:
            import zmq.green as zmq
            import gevent.queue
            QueueType = gevent.queue.Queue
        else:
            import zmq
            import Queue as queue
            QueueType = queue.Queue

        self.database = database
        self.context = zmq.Context()
        self.context.setsockopt(zmq.RCVTIMEO, timeout)
        self.context.setsockopt(zmq.SNDTIMEO, timeout)

        stype = zmq.DEALER if socket_type is None else socket_type

        self.sockets = [self._socket(self.context, stype, host) for _ in xrange(size)]
        self.sockque = QueueType()
        self.sockque.queue.extend(self.sockets)

        self.codes = {
            "\0": self.serialized,
            "\1": self.exception,
            "\2": self.iterable,
            }

    @staticmethod
    def _socket(context, type, host):
        socket = context.socket(type)
        socket.connect(host)
        return socket

    def close(self):
        for socket in self.sockets:
            socket.close()
        self.context.term()

    def command(self, op, args=(), kwargs={}):
        '''
        Run given operation in server.
        :param zmq.Socket socket: ZeroMQ socket for send and receive
        :param basestring database: database name
        :param basestring op: operation name
        :param tuple args: operation arguments
        :param dict kwargs: operation keyword arguments
        :return: data sent by server or None if sync=False
        :raises: if raised by server and found in module or builtins
        :raises Exception: if raised by server and not in module or builtins
        '''
        socket = self.sockque.get()
        try:
            sargs = msgpack.dumps((args, kwargs))
            socket.send_multipart((self.database, op, sargs))
            if kwargs.get("sync", True):
                data_type, data = socket.recv_multipart()
                return self.codes.get(data_type, self.default)(data, op, args, kwargs)
        finally:
            self.sockque.put(socket)


    def serialized(self, data, *_):
        '''
        Convert data sent by server to python objects.

        :param zmq.Socket socket: unused
        :param basestring data: data received from server
        :param *_: unused
        :return: unserialized data
        '''
        return msgpack.loads(data)

    def exception(self, data, *_):
        '''
        Raises exception sent by server.

        :param zmq.Socket socket: unused
        :param basestring data: data received from server
        :param *_: unused
        :raises: if raised by server and found in module or builtins
        :raises Exception: if raised by server and not in module or builtins
        '''
        exc_type, exc_args = msgpack.loads(data)
        for scope in (globals(), __builtin__.__dict__):
            if exc_type in scope:
                raise scope[exc_type](*exc_args)
        raise Exception(*exc_args)

    def iterable(self, data, database, op, args, kwargs):
        '''
        Yield server results, asynchronously from server.

        :param zmq.Socket socket: socket will be used for subsequent requests
        :param basestring data: serialized iter_id send by server
        :param basestring database: database name
        :param basestring op: unused
        :param tuple args: unused
        :param dict kwargs: operation keyword arguments, bulksize key is used
        '''
        iter_id = msgpack.loads(data)
        return RemoteIterable(iter_id, self, kwargs.get("bulksize", 1))

    def default(self, data, *_):
        '''
        Send error to logger as this function is reached when no suitable
        handler is found for server data.

        :param zmq.Socket socket: unused
        :param basestring data: data received from server
        :param *_: unused
        '''
        logger.error("Could not parse server message %r" % data)


class RemoteIterable(object):
    def __init__(self, iter_id, protocol, bulksize=1):
        self.iter_id = iter_id
        self.protocol = protocol
        slef.bulksize = bulksize
        self.cache = collections.deque()
        self.exhausted = False

    def retrieve(self):
        if self.exhausted:
            raise StopIteration
        data = self.protocol.command("iter_next", (self.iter_id, self.bulksize))
        self.cache.extend(data)
        self.exhausted = len(data) < self.bulksize

    def __iter__(self):
        return self

    def next(self):
        if not self.cache:
            self.retrieve()
        return self.cache.popleft()

    def close(self):
        self.pool.close()
        self.context.term()


class LevelDB(object):
    def __init__(self, host, database, timeout=2, green=False, bulksize=10,
                 poolsize=10):
        '''
        Initialize client for given host and database.

        :param host: server URI
        :type host: basestring
        :param database: database name
        :type database: basestring
        :param timeout: send and receive socket timeout in seconds
        :type timeout: int or float
        :param green: Use ZeroMQ 'green' (greenlet friendly) implementation.
        :type green: boolean
        :param bulksize:
        :param poolsize:

        '''
        self.host = host
        self.database = database
        self.bulksize = 10
        self.protocol = CommandProtocol(poolsize, green, host, database,
            -1 if timeout == -1 else int(timeout*1000))

    def command(self, op, args, kwargs):
        return self.protocol.command(op, args, kwargs)

    def get(self, *args, **kwargs):
        return self.command("get", args, kwargs)

    def iterator(self, *args, **kwargs):
        kwargs.setdefault("bulksize", self.bulksize)
        # TODO: choose between iterator and raw_iterator
        return self.command("raw_iterator", args, kwargs)

    def put(self, *args, **kwargs):
        kwargs.setdefault("sync", False)
        return self.command("put", args, kwargs)

    def delete(self, *args, **kwargs):
        kwargs.setdefault("sync", False)
        return self.command("delete", args, kwargs)

    def write(self, operations, **kwargs):
        kwargs.setdefault("sync", False)
        return self.command("get_stats", args, kwargs)

    def close(self):
        self.protocol.close()

    def __del__(self):
        self.close()

logger = logging.getLogger(__name__)
