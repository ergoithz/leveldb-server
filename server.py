#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
import logging
import types

import leveldb

import gevent
import gevent.core
import zmq.green as zmq

__app__ = "High Performance LevelDB Server"
__version__ = "0.2.0"
__author__ = "Felipe A. Hernandez <ergoithz@gmail.com>"
__license__ = "BSD"

class Database(object):
    """
    Database object with commands abstraction layer
    """
    def __init__(self, dbfile, name=None):
        self.name = os.path.basename(dbfile) if name is None else name
        self.db = leveldb.LevelDB(dbfile)

    def get(self, key):
        try:
            return self.db.Get(key)
        except KeyError:
            return None

    def put(self, key, value):
        return self.db.Put(key, value)

    def delete(self, key):
        try:
            return self.db.Delete(key)
        except KeyError:
            return None

    def range(self, start=None, end=None):
        for key, value in self.db.RangeIter(start, end):
            yield key, value


class Worker(object):
    """
    General worker of leveldb server which serve multiple databases
    """
    def __init__(self, context, dbs):
        self.context = context
        self.dbs = dbs

        self.running = True
        self.processing = False
        self.socket = self.context.socket(zmq.XREQ)

        gevent.spawn(self.run)

    def send(self, id, value):
        '''
        Sends value to client with given id taking care of different value type uses cases.

        Type flag::
            \0 None (key not found)
            \1 None (stop iteration)
            \xFE Single return argument
            \xFF Multiple return argument

        '''
        if isinstance(value, basestring):
            self.socket.send_multipart((id, "\xFE", value))
        elif isinstance(value, tuple):
            self.socket.send_multipart((id, "\xFF") + value)
        elif isinstance(value, types.GeneratorType):
            for chunk in value:
                self.send(id, chunk)
            self.socket.send_multipart((id, "\1"))
        elif value is None:
            self.socket.send_multipart((id, "\0"))
        else:
            logging.error("Cannot send value %r" % (value,))

    def run(self):
        self.socket.connect('inproc://backend')

        try:
            while self.running:
                msg = self.socket.recv_multipart()

                self.processing = True
                id, dbname, dbop = msg[:3]
                args = msg[3:]

                logger.debug("Received command from client: %s:%s" % (dbname, dbop))

                self.send(id, getattr(self.dbs[dbname], dbop)(*args))
                self.processing = False
        except zmq.ZMQError as e:
            logging.exception(e)
        self.running = False

    def close(self):
        self.running = False
        while self.processing:
            gevent.sleep()
        self.socket.close()


class Server(object):
    def __init__(self, listen, dbfiles, workers):
        logging.info("Starting leveldb-server %s" % listen)

        self.context = zmq.Context()
        self.frontend = self.context.socket(zmq.XREP)
        self.frontend.bind(listen)
        self.backend = self.context.socket(zmq.XREQ)
        self.backend.bind('inproc://backend')

        self.poll = zmq.Poller()
        self.poll.register(self.frontend, zmq.POLLIN)
        self.poll.register(self.backend,  zmq.POLLIN)

        self.dbs = {db.name: db for db in (Database(dbfile) for dbfile in dbfiles)}

        logger.info("Initialized databases: %s" % ", ".join(self.dbs))

        self.workers = [Worker(self.context, self.dbs) for _ in xrange(workers)]

        gevent.spawn(self.run)

    def run(self):
        while True:
            for socket, type in self.poll.poll():
                if type == zmq.POLLIN:
                    if socket == self.frontend:
                        forward = self.backend
                    elif socket == self.backend:
                        forward = self.frontend
                    else:
                        continue
                    msg = socket.recv_multipart()
                    forward.send_multipart(msg)

    def close(self):
        for worker in self.workers:
            worker.close()
        self.frontend.close()
        self.backend.close()
        self.context.term()

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    import argparse
    import signal

    events_received = []

    def event(event, stack):
        events_received.append(event)
    signal.signal(signal.SIGTERM, event)

    parser = argparse.ArgumentParser(version=__version__, description=__app__)
    parser.add_argument("listen", metavar="tcp://127.0.0.1:5147", help="zmq listen address")
    parser.add_argument("dbfiles", metavar="level.db", nargs="+", help="database files")
    parser.add_argument("-w", "--workers", metavar="N", type=int, default=2, help="number of workers (defaults to 2)")
    parser.add_argument("--verbose", action="store_true", help="show debug messages")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.addHandler(logging.StreamHandler())

    server = Server(args.listen, args.dbfiles, args.workers)

    try:
        while not events_received:
            gevent.sleep()
    except KeyboardInterrupt:
        print " CTRL+C received. Stoping."
    server.close()
