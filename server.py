#Copyright (c) 2011 Fabula Solutions. All rights reserved.
#Use of this source code is governed by a BSD-style license that can be
#found in the license.txt file.

# leveldb server

import os
import logging
import struct

import leveldb
import gevent
import gevent.core
import zmq.green as zmq
import msgpack

class Database(object):
    """
    Database commands abstraction layer
    """
    def __init__(self, dbfile, name=None):
        self.name = os.path.basename(dbfile) if name is None else name
        self.db = leveldb.LevelDB(dbfile)

    def _encode(self, *args):
        # pack into binary protocol
        args = [unicode(a).encode("utf-8") for a in args]

        #return "{0:0=10d}{1}".format(len(data), data)
        return "".join([struct.pack(r"Q", len(data)) + data for data in args])

    def get(self, data):
        try:
            return list(self._encode(self.db.Get(data)))[0]
        except KeyError:
            return self._encode('')

    def put(self, data):
        # should be strings only
        data = [unicode(d) for d in data]
        yield self._encode(self.db.Put(data[0], data[1]))

    def delete(self, data):
        yield self._encode(self.db.Delete(data))

    def range(self, data):
        try:
            if data[0] is None or data[1] is None:
                raise IndexError

            args = [data[0], data[1]]
        except IndexError:
            args = []

        for value in self.db.RangeIter(*args):
            # name, value
            yield self._encode(value[0], value[1])


class Worker(object):
    """
    General worker of leveldb server which
    serve multiple databases and understand
    commands for different databases
    """
    def __init__(self, context, dbs):
        self.context = context
        self.dbs = dbs

        self.running = True
        self.processing = False
        self.socket = self.context.socket(zmq.XREQ)

        gevent.spawn(self)

    def run(self):
        self.socket.connect('inproc://backend')

        # TODO: if/elif looks like pascal-style code,
        # it should be refactored to more clear and simple way
        while self.running:
            try:
                msg = self.socket.recv_multipart()
            except zmq.ZMQError:
                self.running = False
                continue

            self.processing = True

            if len(msg) != 3:
                value = 'None'
                reply = [msg[0], value]
                self.socket.send_multipart(reply)
                continue

            id = msg[0]
            cur_db, op = msg[1].split(":")
            data = msgpack.loads(msg[2])
            reply = [id]

            logger.debug("Received command from client: {}".format(msg[1]))
            db = self.dbs.get(cur_db)
            for chunk in getattr(db, op)(data):
                logger.debug("Generating output: %s" % str(chunk))
                self.socket.send_multipart([msg[0], chunk])

            self.processing = False

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
            frontend = None
            backend = None

            for socket, type in self.poll.poll():
                if socket == self.frontend:
                    frontend = socket, type
                elif socket == self.backend:
                    backend = socket, type

            if frontend and frontend[1] == zmq.POLLIN:
                msg = frontend[0].recv_multipart()
                backend.send_multipart(msg)

            if backend and backend[1] == zmq.POLLIN:
                msg = backend[0].recv_multipart()
                frontend.send_multipart(msg)

    def close(self):
        for worker in self.workers:
            worker.close()
        self.frontend.close()
        self.backend.close()
        self.context.term()

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # force to debug mode

    import argparse

    parser = argparse.ArgumentParser(
        prog='server.py',
        version='0.1.1',
        description='leveldb-server'
    )
    parser.add_argument("--listen", "-l", dest="listen", default="tcp://127.0.0.1:5147")
    parser.add_argument("--workers", "-w", dest="workers", type=int, default=3)
    parser.add_argument("--dbfiles", default="level.db", help="Specify several comma-separated database files")
    parser.add_argument("--verbose", "-v", type=bool, help="Show debug messages")

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.addHandler(logging.StreamHandler())

    server = Server(parser.listen, parser.dbfiles, parser.workers)
    gevent.core.loop()
    server.close()
