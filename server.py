#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
import logging
import types

import gevent
import gevent.threadpool

import leveldb
import zmq.green as zmq

__app__ = "High Performance LevelDB Server"
__version__ = "0.2.0-dev"
__author__ = "Felipe A. Hernandez <ergoithz@gmail.com>"
__license__ = "BSD"

class Database(object):
    '''
    LevelDB abstraction layer.

    Operations runs asynchronously onto a threadpool for not blocking gevent.
    '''
    def __init__(self, dbfile, name=None, pool_size=5):
        self.name = os.path.basename(dbfile) if name is None else name
        self.db = leveldb.LevelDB(dbfile)
        self.pool = gevent.threadpool.ThreadPool(pool_size)

    def async(self, fnc, *args):
        return self.pool.apply_e(BaseException, fnc, args)

    def get(self, key):
        return self.async(self.db.Get, key)

    def put(self, key, value):
        return self.async(self.db.Put, key, value)

    def delete(self, key):
        return self.async(self.db.Delete, key)

    def range(self, start=None, end=None):
        op = self.async(self.db.RangeIter, start, end)
        while True:
            yield self.async(op.next)


class Server(object):
    '''
    Server class.
    '''
    class SendSwitchType(object):
        '''
        Python switch-like for sending data..
        '''
        def __init__(self):
            self.options = {
                # Types
                types.NoneType: self.none,
                types.StringType: self.string,
                types.TupleType: self.string_tuple,
                types.GeneratorType: self.generator,
                KeyError: self.key_error,
                }

        def __call__(self, socket, id, value):
            self.options.get(type(value), self.default)(socket, id, value)

        def key_error(self, socket, id, value):
            socket.send_multipart((id, "\2", value.message) + value.args)

        def string(self, socket, id, value):
            socket.send_multipart((id, "\xFE", value))

        def string_tuple(self, socket, id, value):
            socket.send_multipart((id, "\xFF") + value)

        def generator(self, socket, id, value):
            for chunk in value:
                self(socket, id, chunk)
            socket.send_multipart((id, "\1"))

        def none(self, socket, id, value):
            socket.send_multipart((id, "\0"))

        def default(self, socket, id, value):
            if isinstance(value, BaseException):
                # Send generic exception
                socket.send_multipart((id, "\xFB", value.message) + value.args)
            else:
                logging.error("Cannot send value %r" % (value,))

    send = staticmethod(SendSwitchType())

    def __init__(self, listen, dbfiles):
        logging.info("Starting leveldb-server %s" % listen)

        self.context = zmq.Context()
        self.frontend = self.context.socket(zmq.XREP)
        self.frontend.bind(listen)

        self.poll = zmq.Poller()
        self.poll.register(self.frontend, zmq.POLLIN)

        self.dbs = {db.name: db for db in (Database(dbfile) for dbfile in dbfiles)}

        logger.info("Initialized databases: %s" % ", ".join(self.dbs))

        self._run = False
        self._running = False

    def recv(self, socket):
        message = socket.recv_multipart()
        id, dbname, dbop = message[:3]
        return id, dbname, dbop, message[3:]

    def task(self, socket):
        id, dbname, dbop, args = self.recv(socket)
        logger.debug("Received command from client: %s:%s" % (dbname, dbop))
        try:
            data = getattr(self.dbs[dbname], dbop)(*args)
        except BaseException as e:
            data = e
        self.send(socket, id, data)

    def main(self):
        '''
        Server mainloop.
        '''
        self._run = True
        self._running = True
        while self._run:
            for socket, type in self.poll.poll(timeout=1):
                self.task(socket)
        self._running = False

    def stop(self):
        '''
        Stop mainloop
        '''
        self._run = False

    def close(self):
        '''
        Stop mainloop if running and close descriptors.
        '''
        self.stop()
        while self._running:
            gevent.sleep()
        self.frontend.close()
        self.context.term()

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    import argparse
    import signal

    def event(event, stack):
        server.stop()

    signal.signal(signal.SIGTERM, event)

    parser = argparse.ArgumentParser(version=__version__, description=__app__)
    parser.add_argument("listen", metavar="tcp://127.0.0.1:5147", help="zmq listen address")
    parser.add_argument("dbfiles", metavar="level.db", nargs="+", help="database files")
    parser.add_argument("--verbose", action="store_true", help="show debug messages")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.addHandler(logging.StreamHandler())

    server = Server(args.listen, args.dbfiles)
    try:
        server.main()
    except KeyboardInterrupt:
        print " CTRL+C received. Stoping."
    server.close()