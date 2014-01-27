#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
ZeroMQ LevelDB Server
A python ZeroMQ LevelDB Server.

Current py-LevelDB implementation is a bottleneck for any sophisticaced,
thread-based, gevent-based or asynchronous server, thus after a lot of
benchmarking I decided to trim down and reduce server code to minimum.
'''

import os
import os.path
import signal
import logging
import errno
import time
import functools
import collections

import msgpack
import leveldb
import gevent
import zmq.green as zmq

__app__ = "ZeroMQ LevelDB Server"
__version__ = "0.2.0-dev"
__author__ = "Felipe A. Hernandez <ergoithz@gmail.com>"
__license__ = "BSD"

class Database(object):
    '''
    LevelDB abstraction layer.

    LevelDB methods have ugly names, this class wraps them with pythonic ones,
    lowercased and underscored instead of capitalized and camelcased. This way,
    'Get' becomes 'get' and 'RangeIter' becomes 'range_iter'.

    Also, write method now can receive an iterable of tuples, which has better
    interoperability.
    '''
    def __init__(self, filename, *args, **kwargs):
        '''
        :param name: global name of this database for clients.
        :type name: basestring or None

        :param dbfile: LevelDB database path
        :type dbfile: basestring

        '''
        self.db = leveldb.LevelDB(filename, *args, **kwargs)
        self.iterables = collections.OrderedDict()

    def __getattr__(self, name):
        '''
        Lower and underscored attribute names are reformatted, searched in
        'db' and cached in object instance.
        '''
        if name.islower():
            if "_" in name:
                name = name.replace("_", " ").title().replace(" ", "")
            else:
                name = name.capitalize()
            attr = getattr(self.db, name)
            if name.endswith("Iter"):
                attr = functools.partial(self.iter_wrapper, attr)
            setattr(self, name, attr)
            return attr
        return object.__getattr__(self, name)

    def iter_wrapper(self, fnc, *args, **kwargs):
        if self.iterables:
            name = (next(reversed(self.iterables)) + 1) % sys.maxint
        else:
            name = 0
        self.iterables[name] = TimedIterable(name, fnc(*args, **kwargs))
        return name

    def iter_next(self, name):
        '''

        '''
        data = self.iterables[name].next()
        self.iterables[name] = self.iterables.pop(name)
        return data

    def write(self, write_batch_list, sync=False):
        '''
        apply multiple put/delete operations atomatically
        write_batch: the WriteBatch list as [("put", key, value), ("delete", key)...]
                      holding the operations
        '''
        if isinstance(write_batch_list, leveldb.WriteBatch):
            write_batch = write_batch_list
        else:
            write_batch = leveldb.WriteBatch()
            for task in write_batch_list:
                if task[0] == "put":
                    write_batch.Put(task[1], task[2])
                elif task[0] == "delete":
                    write_batch.Delete(task[1])
        return leveldb.LevelDB.Write(self, write_batch, sync)


class ClientException(Exception):
    pass


class TimedIterable(object):
    def __init__(self, name, source):
        self.lt = time.time()
        self.name = name
        self.source = source

    def __iter__(self):
        return self

    def next(self):
        self.lt = time.time()
        return self.source.next()


class Server(object):
    '''
    Server class.
    '''

    def __init__(self, listen, dbfiles, timeout = 10):
        logging.info("Starting leveldb-server %s" % listen)

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.XREP)
        self.socket.bind(listen)

        self.dbs = {os.path.basename(path): Database(path) for path in dbfiles}

        self.timeout = timeout

        logger.info("Initialized databases: %s" % ", ".join(self.dbs))

        self._run = False
        self._running = False

    def cleaner(self):
        while self._running:
            t = time.time()
            for db in self.dbs.itervalues():
                while db.iterables:
                    genid = next(db.iterables)
                    if self.iterables[genid].lt > t:
                        # TODO: Kill leveldb-iterator?
                        del db.iterables[genid]
                        continue
                    break
            gevent.sleep(1)

    def async_generator(self, iterable, identifier):
        with gevent.Timeout(self.timeout):
            for item in iterable:
                socket.send_multipart((id, ord(identifier+128), msgpack.dumps(data)))

    def task(self, socket, message):
        '''
        Respond to given message using given socket.
        '''
        try:
            id, dbname, dbop = message[:3]
            args, kwargs = msgpack.loads(message[3])
            data = getattr(self.dbs[dbname], dbop)(*args, **kwargs)
            data_type = "\2" if dbop.endswith("_iter") else "\0"
        except ClientException as e:
            data = (e.args[0], e.args[1:])
            data_type = "\1"
        except BaseException as e:
            data = (e.__class__.__name__, e.args)
            data_type = "\1"
        socket.send_multipart((id, data_type, msgpack.dumps(data)))

    def main(self):
        '''
        Server mainloop.
        '''
        self._running = True
        gevent.spawn(self.cleaner).start()
        while True:
            try:
                message = self.socket.recv_multipart()
                self.task(self.socket, message)
            except zmq.ZMQError as e:
                if err.errno == errno.EINTR:
                    break
                logging.exception(e)
        self._running = False

    def stop(self):
        '''
        Send signal to current process for raising interrupting ZMQError.
        '''
        if self._running:
            # Signals are handled by ZMQ and throws ZMQError with errorno EINTR
            os.kill(os.getpid(), signal.SIGTERM)

    def close(self):
        '''
        Stop mainloop if running and close descriptors.
        '''
        self.stop()
        self.socket.close()
        self.context.term()

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    import argparse

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
