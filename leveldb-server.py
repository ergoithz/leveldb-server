#Copyright (c) 2011 Fabula Solutions. All rights reserved.
#Use of this source code is governed by a BSD-style license that can be
#found in the license.txt file.

# leveldb server
import leveldb
import json
import optparse
import os
import threading
import time
import sys
import zmq
import logging
import struct

logger = logging.getLogger("leveldb-server")

USAGE = """
python leveldb-server.py
"""


class Commands(object):
    """
    Database commands abstraction layer
    """
    def __init__(self, db):
        self.db = db

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


class WorkerThread(threading.Thread):
    """
    General worker of leveldb server which
    serve multiple databases and understand
    commands for different databases
    """
    def __init__(self, context, dbs):
        threading.Thread.__init__(self)

        self.context = context
        self.dbs = {name: Commands(db) for db, name in dbs}

        self.running = True
        self.processing = False
        self.socket = self.context.socket(zmq.XREQ)

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
            data = json.loads(msg[2])
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
            time.sleep(1)
        self.socket.close()


def initialize(options):
    """
    Initialize LevelDB Server
    """
    print "Starting leveldb-server %s" % options.listen
    context = zmq.Context()
    frontend = context.socket(zmq.XREP)
    frontend.bind(options.listen)
    backend = context.socket(zmq.XREQ)
    backend.bind('inproc://backend')

    poll = zmq.Poller()
    poll.register(frontend, zmq.POLLIN)
    poll.register(backend,  zmq.POLLIN)

    workers = []
    dbs = []

    # NB: iterate trhought dbs
    for dbfile in options.dbfiles.split(","):
        name = os.path.basename(dbfile)
        dbs.append((leveldb.LevelDB(dbfile), name))

    print "Initialized databases: {}".format(
        ", ".join(map(lambda d: d[1], dbs)))

    # add all workers inside of workers to correctly shutdown later
    for i in xrange(options.workers):
        worker = WorkerThread(context, dbs)
        worker.start()
        workers.append(worker)

    try:
        while True:
            sockets = dict(poll.poll())
            if frontend in sockets:
                if sockets[frontend] == zmq.POLLIN:
                    msg = frontend.recv_multipart()
                    backend.send_multipart(msg)

            if backend in sockets:
                if sockets[backend] == zmq.POLLIN:
                    msg = backend.recv_multipart()
                    frontend.send_multipart(msg)
    except KeyboardInterrupt:
        for worker in workers:
            worker.close()
        frontend.close()
        backend.close()
        context.term()


if __name__ == "__main__":
    # force to debug mode
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    optparser = optparse.OptionParser(
        prog='leveldb-server.py',
        version='0.1.1',
        description='leveldb-server',
        usage=USAGE)
    optparser.add_option(
        '--listen', '-l', dest='listen',
        default='tcp://127.0.0.1:5147')
    optparser.add_option(
        '--workers', '-w', dest='workers', type=int,
        default=3)

    optparser.add_option(
        '--dbfiles',
        default='level.db',
        help="Specify several comma-separated database files")
    options, arguments = optparser.parse_args()

    if not (options.listen and options.dbfiles):
        optparser.print_help()
        sys.exit(1)

    initialize(options)
