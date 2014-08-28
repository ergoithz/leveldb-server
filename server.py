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
import signal

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
    RemoteObjectTypes = (
        plyvel._plyvel.Iterator,
        plyvel._plyvel.RawIterator,
        plyvel._plyvel.PrefixedDB,
        plyvel._plyvel.WriteBatch,
        plyvel._plyvel.Snapshot,
        )
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
        self.last_numid = 0
        self.remote_objects = {}
        self.remote_objects_lt = collections.OrderedDict()

    def ro_expire(self, roid=None, lt=0):
        if roid is None:
            success = False
            while self.remote_objects_lt:
                roid = self.remote_objects_lt.iterkeys().next()
                if not self.ro_expire(roid, lt):
                    break
                success |= True
            return success
        if self.remote_objects_lt[roid] < lt:
            self.ro_close(roid)
            return True
        return False

    def ro_close(self, roid):
        '''
        Close remote object
        '''
        try:
            del self.remote_objects_lt[roid]
            remote_object = self.remote_objects.pop(roid)
            if hasattr(remote_object, "close"):
                return remote_object.close()
        except KeyError:
            raise ClientException("RemoteObjectInvalidError")

    def ro_bump(self, roid):
        '''
        Update last-touched dict for given roid
        '''
        try:
            del self.remote_objects_lt[roid]
            self.remote_objects_lt[roid] = time.time()
        except KeyError:
            raise ClientException("RemoteObjectInvalidError")

    def ro_wrapper(self, remote_object, client_id):
        '''
        Call given function with given args kwargs, creates an AsyncIterable
        and store it and returns iterable name. See :py:method:Database.iter_next
        '''
        # ClientID is attached to identifier avoiding clients mixing remote
        # objects on extremely busy servers.
        self.last_numid = numid = (int(self.last_numid) + 1) % 4294967295 # max uint is 2**32-1
        roid = "%d.%s" % (numid, client_id)
        self.remote_objects[roid] = remote_object
        self.remote_objects_lt[roid] = time.time()
        return remote_object.__class__.__name__, roid

    def ro_next(self, roid, howmany, reverse=False):
        '''
        Call multiple
        '''
        self.ro_bump(roid)
        iterable = self.remote_objects[roid]
        if reverse:
            iterable = self._iter_prev(iterable)
        return list(itertools.islice(iterable, howmany))

    def ro_method(self, roid, method, *args, **kwargs):
        '''
        Call method on stored remote object
        '''
        if method == "close":
            return self.ro_close(roid)
        self.ro_bump(roid)
        return getattr(self.remote_objects[roid], method)(*args, **kwargs)

    @classmethod
    def _iter_prev(cls, iterable):
        '''
        Yield iterable using its `prev` method
        '''
        try:
            while True:
                yield iterable.prev()
        except StopIteration:
            pass

    @classmethod
    def _fix_arguments(cls, op, args, kwargs):
        '''
        Plyvel does not allow to pass positional arguments as keyword arguments
        (which is the expected python behavior), this method aims to fix this,
        and allow to pass keyword arguments as positional ones too.
        '''
        proto_args, proto_kwargs = cls.db_methods[op]
        if proto_args is None:
            # If proto_args is None method has a positional wildcard
            given_args = args
            given_kwargs = kwargs
        elif len(args) > len(proto_args):
            pnargs = len(proto_args)
            # Move args to kwargs
            given_args = args[:pnargs]
            given_kwargs = dict(itertools.izip(proto_kwargs, args[pnargs:]))
             # Avoid duplicates in proto_kwargs corresponding to extra args
            for keyword in given_kwargs:
                if keyword in kwargs:
                    raise TypeError(
                        "%s() got multiple values for keyword argument %r"
                            % (op, keyword))
            # Add kwargs
            given_kwargs.update(kwargs)
        else:
            # Move kwargs to args
            gnargs = len(args)
            given_args = list(args)
            given_kwargs = dict(kwargs)
            try:
                given_args.extend(given_kwargs.pop(k) for k in proto_args[gnargs:])
            except KeyError:
                pnargs = len(proto_args)
                raise TypeError(
                    "%s() takes at least %d argument%s (%d given)"
                        % (op, pnargs, "s" if pnargs > 1 else "", gnargs))
        return given_args, given_kwargs

    def command(self, op, *args, **kwargs):
        if op in self.not_implemented:
            raise NotImplementedError, self.not_implemented[op]
        elif hasattr(self, op):
            return getattr(self, op)(*args, **kwargs)
        elif op in self.db_methods:
            # Fix methods arguments and keyword arguments by position
            args, kwargs = self._fix_arguments(op, args, kwargs)
        return getattr(self.db, op)(*args, **kwargs)


class ClientException(Exception):
    pass


class Server(object):
    '''
    Server class.
    '''
    def __init__(self, listen, dbfiles, timeout = 0.5, rotimeout = 10, poolsize=10, **kwargs):
        '''
        Initialize LevelDB database objects and ZeroMQ sockets.

        :param basestring listen: zmq listen address
        :param iterable dbfiles: iterable of database paths
        :param float timeout: seconds after a request will be cut by server
        :param float rotimeout: seconds of innactivity for a remote object after being garbage collected
        :param *: extra arguments will be passed to :py:class:Database constructors
        '''
        self.timeout = timeout
        self.rotimeout = rotimeout
        self.context = zmq.Context()
        self.context.setsockopt(zmq.RCVTIMEO, int(self.timeout*1000))
        self.context.setsockopt(zmq.SNDTIMEO, int(self.timeout*1000))
        self.context.setsockopt(zmq.LINGER, int(self.timeout*1000))

        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind(listen)

        self.backend = self.context.socket(zmq.DEALER)
        self.backend.bind('inproc://backend')

        self.workers = [self.context.socket(zmq.DEALER) for _ in xrange(poolsize)]
        for socket in self.workers:
            socket.connect('inproc://backend')

        logger.info("Listening at %s" % listen)

        self.dbs = {os.path.basename(path): Database(path, **kwargs)
                    for path in dbfiles}
        self.tasks = []

        logger.info("Database%s: %s" % ("s" if len(self.dbs) > 1 else "",
                                        os.pathsep.join(self.dbs)))

    def janitor(self):
        '''
        While mainloop is running, perform periodic cleans or timeout'd
        remote objects.
        '''
        try:
            while True:
                expiration = time.time() - self.rotimeout
                for db in self.dbs.itervalues():
                    db.ro_expire(lt=expiration)
                gevent.sleep(self.rotimeout)
        except gevent.GreenletExit:
            logger.info("Bye, janitor.")

    def task(self, socket, message):
        '''
        Respond depending on given message using given socket.

        Messages are iterables of the following strings:
         * id: client id ( for ZeroMQ Router/Dealer interaction)
         * database name: database basename
         * database operation: database operation (usually method) will be performed
         * args and kwargs: msgpack iterable of two elements:
            * iterable of arguments
            * dictionary of keyword arguments

        Note the response is written on socked depending if 'wait' keyword
        argument is not given or evaluates to True.

        :param zmq.Socket socket: Socket where response will be, hopefully, sent.
        :param iterable message: Iterable of strings for messages.

        '''
        # intentionally outside try-except
        id, dbname, dbop = message[:3]
        args, kwargs = msgpack.loads(message[3])
        do_response = kwargs.pop("wait", True)
        if "bulksize" in kwargs:
            del kwargs["bulksize"]
        try:
            data = self.dbs[dbname].command(dbop, *args, **kwargs)
            data_type = "\0"
            if isinstance(data, Database.RemoteObjectTypes):
                data = self.dbs[dbname].ro_wrapper(data, id)
                data_type = "\2"
        except ClientException as e:
            data = (e.args[0], e.args[1:])
            data_type = "\1"
        except BaseException as e:
            data = (e.__class__.__name__, e.args)
            data_type = "\1"
        if do_response:
            socket.send_multipart((id, data_type, msgpack.dumps(data)))

    def worker(self, socket):
        '''
        Call :py:method:task for every message received from given socket.

        For some messages, a response will be sent using the same socket (see :py:method:task).

        Blocks until exception is raised, expecting a gevent.GreenletExit for terminating gracefully.

        :param zmq.Socket socket: Socket for incoming messages.
        '''
        try:
            while True:
                try:
                    message = socket.recv_multipart()
                    self.task(socket, message)
                except zmq.ZMQError as e:
                    if err.errno == errno.EINTR:
                        break
                    logger.exception(e)
                except gevent.GreenletExit:
                    raise
                except BaseException as e:
                    logger.exception(e)
        except gevent.GreenletExit:
            logger.info("Bye, worker.")

    def proxy(self, in_socket, out_socket):
        '''
        Takes two sockets and forward messages between them, in one direction.

        Blocks until exception is raised, expecting a gevent.GreenletExit for terminating gracefully.

        :param zmq.Socket in_socket: Socket from messages will be retrieved
        :param zmq.Socket out_socket: Socket where messages will be sent
        '''
        try:
            while True:
                try:
                    out_socket.send_multipart(in_socket.recv_multipart())
                except zmq.ZMQError as e:
                    # TODO: handle exception
                    logger.error("ZMQError received %d: %r." % (e.errno, e.msg))
        except gevent.GreenletExit:
            logger.info("Bye, proxy.")

    def start(self):
        '''
        Start server tasks (corroutines).

        The following method (with their own mainloop) are spawned:
            :py:method:janitor Remote method timeout garbage collectior.
            :py:method:proxy Two instances: one for ingoing messages and another for outgoing ones.
            :py:method:worker One handler for each initialized worker socket.

        This method is non-blocking. For a blocking version see :py:method:main method.
        '''
        self.stop()
        tasks = [
            gevent.spawn(self.janitor),
            # Proxies
            gevent.spawn(self.proxy, self.socket, self.backend),
            gevent.spawn(self.proxy, self.backend, self.socket),
            ]
        # Workers
        tasks.extend(gevent.spawn(self.worker, s) for s in self.workers)
        self.tasks[:] = tasks

    def main(self):
        '''
        Start server (if not already running) and wait until :py:method:stop is called or (unlikely, see :py:method:start) all tasks finish.
        '''
        if not self.tasks:
            self.start()
        gevent.joinall(self.tasks)

    def signal(self, signal, frame):
        '''
        System signal handler.

        Note python signals cannot call :py:meth:stop directly because they don't run on a gevent managed environment, so need to spawn a :py:class:gevent.Greenlet  for :py:meth:stop .

        :param int signal: System received signal
        :param frame frame: Frame object or None
        '''
        logger.info("Signal %d received. Stopping." % signal)
        gevent.spawn(server.stop)

    def stop(self):
        '''
        Stop running tasks (corroutines), this means :py:method:main will return (if running) on next gevent switch.
        '''
        if self.tasks:
            gevent.killall(self.tasks)
            del self.tasks[:]

    def close(self):
        '''
        Stop tasks if running and close descriptors.
        '''
        self.stop()
        for socket in self.workers:
            socket.close()
        self.backend.close()
        self.socket.close()
        self.context.term()
        logger.info("Server closed.")


logger = logging.getLogger(__name__)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(version=__version__, description=__app__)
    parser.add_argument("listen", metavar="ADDRESS",
        help="zmq listen address like tcp://127.0.0.1:5147")
    parser.add_argument("dbfiles", metavar="PATH", nargs="+",
        help="database directory path")
    parser.add_argument("--timeout", default=0.5, type=float,
        metavar="SECONDS", help="request timeout")
    parser.add_argument("--rotimeout", default=10, type=float,
        metavar="SECONDS", help="remote object timeout")
    parser.add_argument("--create-if-missing", default=False,
        action="store_true", help="create database if not exists")
    parser.add_argument("--verbose", action="store_true",
        help="show debug messages")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.addHandler(logging.StreamHandler())

    server = Server(args.listen, args.dbfiles,
        create_if_missing = args.create_if_missing,
        timeout = args.timeout,
        rotimeout = args.rotimeout
        )
    signal.signal(signal.SIGTERM, server.signal)
    try:
        server.main()
    except KeyboardInterrupt:
        logger.info("CTRL+C received. Stopping.")
    server.close()
