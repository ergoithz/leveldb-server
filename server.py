#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
ZeroMQ LevelDB Server
A python ZeroMQ LevelDB Server.

LevelDB is a bottleneck for any sophisticaced, thread-based, gevent-based or
asynchronous server, thus after a lot of benchmarking I decided to trim down
and reduce server code to minimum: One mainloop doing some parsing and
forwarding request to LevelDB as soon as possible.

Gevent is used for running garbage collection of orphan generators.
'''

import os
import os.path
import signal
import logging
import errno
import time
import functools
import itertools
import collections

import msgpack
import plyvel
import gevent
import zmq.green as zmq

__app__ = "ZeroMQ LevelDB Server"
__version__ = "0.2.0-dev"
__author__ = "Felipe A. Hernandez <ergoithz@gmail.com>"
__license__ = "BSD"

class Database(object):
    '''
    LevelDB abstraction layer.

    Also, write method now can receive an iterable of tuples, which has better
    interoperability.
    '''
    IteratorTypes = (plyvel._plyvel.Iterator, plyvel._plyvel.RawIterator)
    db_methods = {
        # plyvel.DB method signatures {method: (args tuple..., kwargs tuple...))}
        "get_property": (("name",),()),
        "write_batch": ((), ("transaction", "sync")),
        "compact_range": ((), ("start", "stop")),
        "iterator": ((), ("reverse", "start", "stop", "include_start", "include_stop", "prefix", "include_key", "include_value", "verify_checksums", "fill_cache")),
        "get": (("key",), ("default", "verify_checksums", "fill_cache")),
        "snapshot": ((), ()),
        "raw_iterator": ((), ("verify_checksums", "fill_cache")),
        "put": (("key", "value"), ("sync",)),
        "close": ((), ()),
        "approximate_size": (("start", "stop"), ()),
        "prefixed_db": (("prefix",), ()),
        "approximate_sizes": (None, ()),
        "delete": (("key",), ("sync",)),
    }


    def __init__(self, name, create_if_missing=False, error_if_exists=False,
        paranoid_checks=None, write_buffer_size=None, max_open_files=None,
        lru_cache_size=None, block_size=None, block_restart_interval=None,
        compression='snappy', bloom_filter_bits=0, comparator=None,
        comparator_name=None):
        '''
        :param basestring name: name of the database (directory name)
        :param bool create_if_missing: whether a new database should be created if needed
        :param bool error_if_exists: whether to raise an exception if the database already exists
        :param bool paranoid_checks: whether to enable paranoid checks
        :param int write_buffer_size: size of the write buffer (in bytes)
        :param int max_open_files: maximum number of files to keep open
        :param int lru_cache_size: size of the LRU cache (in bytes)
        :param int block_size: block size (in bytes)
        :param int block_restart_interval: block restart interval for delta encoding of keys
        :param bool compression: whether to use Snappy compression (enabled by default)
        :param int bloom_filter_bits: the number of bits to use for a bloom filter; the default of 0 means that no bloom filter will be used
        :param ccallable comparator: a custom comparator callable that takes to byte strings and returns an integer
        :param str comparator_name: name for the custom comparator
        '''
        self.db = plyvel.DB(name, create_if_missing=create_if_missing,
            error_if_exists=error_if_exists, paranoid_checks=paranoid_checks,
            write_buffer_size=write_buffer_size, max_open_files=max_open_files,
            lru_cache_size=lru_cache_size, block_size=block_size,
            block_restart_interval=block_restart_interval,
            compression=compression, bloom_filter_bits=bloom_filter_bits,
            comparator=comparator, comparator_name=comparator_name)
        self.not_implemented = {
            "prefixed_db": "Not implemented by server.",
            "write_batch": "Not implemented by server, use 'write' instead.",
            }
        self.iterables = {}
        self.iterables_lt = collections.OrderedDict()

    def iter_wrapper(self, iterator, client_id):
        '''
        Call given function with given args kwargs, creates an AsyncIterable
        and store it and returns iterable name. See :py:method:Database.iter_next
        '''
        clientid = kwargs.pop("clientid", 1)
        # ClientID is attached to iter name to avoid other client taking apart
        # on extremely busy servers.
        if self.iterables:
            last_numid, last_client = next(reversed(self.iterables)).split(".")
            numid = (int(last_numid) + 1) % 4294967295 # max uint is 2**32-1
        else:
            numid = 0
        iter_id = "%d.%s" % (numid, client_id)
        self.iterables[iter_id] = iterator
        self.iterables_lt[iter_id] = time.time()
        return iter_id

    def iter_next(self, iter_id, method, howmany):
        del self.iterables_lt[iter_id]
        data = list(itertools.islice(self.iterables[iter_id], howmany))
        if len(data) < howmany:
            self.iterables.pop(iter_id).close()
        else:
            self.iterables_lt[iter_id] = time.time()
        return data

    def iter_method(self, iter_id, method, *args, **kwargs):
        '''

        '''
        del self.iterables_lt[iter_id]
        if method == "close":
            data = self.iterables.pop(iter_id).close()
        else:
            data = getattr(self.iterables[iter_id], method)(*args, **kwargs)
            self.iterables_lt[iter_id] = time.time()
        return data

    def command(self, op, *args, **kwargs):
        if op in self.not_implemented:
            raise NotImplementedError, self.not_implemented[op]
        elif hasattr(self, op):
            return getattr(self, op)(*args, **kwargs)
        elif op in self.db_methods:
            # Fix methods arguments and keyword arguments by position
            proto_args, proto_kwargs = self.db_methods[op]
            if proto_args is None:
                given_args = args
                given_kwargs = {}
            elif len(args) > len(proto_args):
                # Move args to kwargs by position
                nargs = len(proto_args)
                given_args = args[:nargs]
                given_kwargs = dict(itertools.izip(proto_kwargs, args[nargs:]))
            else:
                # Move kwargs to args by position
                nargs = len(args)
                given_args = list(args)
                given_args.extend(kwargs[k] for k in proto_args[len(args):])
                given_kwargs = {k:v for k,v in kwargs.iteritems() if not k in proto_args}
            args = given_args
            kwargs = given_kwargs
        return getattr(self.db, op)(*args, **kwargs)

    def write(self, write_batch_list, transaction=False, sync=False):
        '''
        apply multiple put/delete operations atomatically
        write_batch: the WriteBatch list as [("put", key, value), ("delete", key)...]
                      holding the operations
        '''
        with plyvel.DB.write_batch(self, transaction, sync) as w:
            for task in write_batch_list:
                getattr(w, task[0])(*task[1:])


class ClientException(Exception):
    pass


class Server(object):
    '''
    Server class.
    '''
    def __init__(self, listen, dbfiles, timeout = 10, **kwargs):
        '''

        '''
        self.timeout = timeout
        self.context = zmq.Context()
        self.context.setsockopt(zmq.RCVTIMEO, (self.timeout*1000))
        self.context.setsockopt(zmq.SNDTIMEO, (self.timeout*1000))

        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind(listen)

        logger.info("Listening at %s" % listen)

        self.dbs = {os.path.basename(path): Database(path, **kwargs)
                    for path in dbfiles}
        logger.info("Database%s: %s" % ("s" if len(self.dbs) > 1 else "",
                                        ", ".join(self.dbs)))
        self._run = False
        self._running = False

    def cleaner(self):
        '''
        While mainloop is running, perform periodic cleans
        '''
        while self._running:
            expiration = time.time() - self.timeout
            for db in self.dbs.itervalues():
                while db.iterables_lt:
                    numid, lt = db.iterables_lt.iteritems().next()
                    if lt < expiration:
                        db.iterables.pop(numid).close()
                        del db.iterables_lt[numid]
                        continue
                    break
            gevent.sleep(self.timeout)

    def task(self, socket, message):
        '''
        Respond to given message using given socket.
        '''
        # intentionally outside try-except
        id, dbname, dbop = message[:3]
        args, kwargs = msgpack.loads(message[3])
        try:
            data = self.dbs[dbname].command(dbop, *args, **kwargs)
            data_type = "\0"
            if isinstance(data, Database.IteratorTypes):
                data = self.dbs[dbname].iter_wrapper(data)
                data_type = "\2"
        except ClientException as e:
            data = (e.args[0], e.args[1:])
            data_type = "\1"
        except BaseException as e:
            data = (e.__class__.__name__, e.args)
            data_type = "\1"
        if kwargs.get("sync", True):
            socket.send_multipart((id, data_type, msgpack.dumps(data)))

    def main(self):
        '''
        Server mainloop.
        '''
        self._running = True
        self._run = True
        gevent.spawn(self.cleaner).start()
        while self._run:
            try:
                message = self.socket.recv_multipart()
                self.task(self.socket, message)
            except zmq.ZMQError as e:
                if err.errno == errno.EINTR:
                    break
                logger.exception(e)
            except BaseException as e:
                logger.exception(e)
        self._running = False

    def stop(self):
        '''
        Send signal to current process for raising interrupting ZMQError.
        '''
        if self._running:
            self._run = False
            # Signals are handled by ZMQ and throws ZMQError with errorno EINTR
            os.kill(os.getpid(), signal.SIGTERM)

    def close(self):
        '''
        Stop mainloop if running and close descriptors.
        '''
        self.stop()
        self.socket.close()
        self.context.term()
        logger.info("Server closed.")

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(version=__version__, description=__app__)
    parser.add_argument("listen", metavar="tcp://127.0.0.1:5147", help="zmq listen address")
    parser.add_argument("dbfiles", metavar="level.db", nargs="+", help="database files")
    parser.add_argument("--create-if-missing", default=False, action="store_true", help="show debug messages")
    parser.add_argument("--verbose", action="store_true", help="show debug messages")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.addHandler(logging.StreamHandler())

    server = Server(args.listen, args.dbfiles,
        create_if_missing=args.create_if_missing)
    try:
        server.main()
    except KeyboardInterrupt:
        print " CTRL+C received. Stoping."
    server.close()
