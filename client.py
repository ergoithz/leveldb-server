#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import functools
import collections
import logging
import sys
import __builtin__

import msgpack


class Error(Exception):
    '''
    Generic LevelDB error

    This class is also the "parent" error for other LevelDB errors
    (:py:exc:`IOError` and :py:exc:`CorruptionError`). Other exceptions from this
    module extend from this class.
    '''
    pass


class IOError(Error, IOError):
    '''
    LevelDB IO error

    This class extends both the main LevelDB Error class from this
    module and Python's built-in IOError.
    '''
    pass


class CorruptionError(Error):
    '''
    LevelDB corruption error
    '''
    pass


class RemoteObjectInvalidError(Error):
    '''
    Used for all remote objects when there is no matching object on server.
    '''
    pass


class IteratorInvalidError(RemoteObjectInvalidError):
    '''
    Used by :py:class:`RawIterator` to signal invalid iterator state.
    '''
    pass


def remote_method(name, doc, *default_args, **default_kwargs):
    '''
    Create method which call to method 'command' with given args and kwargs.

    '''
    callback = default_kwargs.pop("callback", None)
    prepare = default_kwargs.pop("prepare", None)
    def method(self, *args, **kwargs):
        if default_args:
            op_args = list(default_args)
            op_args.extend(args)
        else:
            op_args = args
        if default_kwargs:
            op_kwargs = dict(default_kwargs)
            op_kwargs.update(kwargs)
        else:
            op_kwargs = kwargs
        if prepare:
            getattr(self, prepare)()
        data = self.command(name, op_args, op_kwargs)
        if callback:
            getattr(self, callback)(data)
        return data
    method.__name__ = name
    method.__doc__ = doc
    return method


class RemoteObject(object):
    '''
    Client side of object on server.

    Note remote objects are continuously garbage collected on server side, so
    no object will survive after an inactivity period bigger than the remote
    object timeout specified on server.
    '''
    def __init__(self, rotype, roid, command):
        self.rotype = rotype
        self.roid = roid
        self.protocol, self._op, self._args, self._kwargs = command
        self.closed = False

    def command(self, op, args=(), kwargs={}):
        '''
        Run command on server for this remote object

        :param basestring op: operation
        :param iterable args: arguments
        :param dict kwargs: keyword arguments
        '''
        op_args = [self.roid, op]
        op_args.extend(args)
        return self.protocol.command("ro_method", op_args, kwargs)

    def close(self):
        '''
        Remove object on server.
        '''
        if not self.closed:
            self.closed = True
            self.protocol.command("ro_close", (self.roid,), {"wait": False})

    def __repr__(self):
        return "%s for remote %s>" % (object.__repr__(self)[-1], self.roid)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class BaseIterator(RemoteObject):
    '''
    Base for remote iterator objects
    '''
    def __init__(self, rotype, roid, command):
        RemoteObject.__init__(self, rotype, roid, command)
        self.bulksize = self._kwargs.get("bulksize", 10)
        self._cache = collections.deque()
        self._cache_reversed = False
        self._exhausted = False
        self._reversed = self._kwargs.get("reverse", False)

    def __iter__(self):
        return self

    def _retrieve(self, reverse=False):
        '''
        If there is no results on self._cache, get more from server.
        '''
        if self._cache:
            return

        if self._exhausted:
            raise StopIteration

        # Decide if retrieve backwards or not, and clean cache on change
        reversed = reverse != self._reversed
        if reversed != self._cache_reversed:
            self._clear()

        args = (self.roid, self.bulksize, reversed)
        data = self.protocol.command("ro_next", args)
        self._exhausted = len(data) < self.bulksize
        self._cache_reversed = reversed
        if not data:
            raise StopIteration
        self._cache.extend(data)

    def _clear(self):
        self._cache.clear()
        self._exhausted = False
        self._cache_reversed = False

    def next(self):
        '''
        Move the iterator one step forward.

        May raise :py:exc:`IteratorInvalidError`.
        '''
        self._retrieve()
        return self._cache.popleft()

    def prev(self):
        '''
        Move one step back and return the previous entry.

        This returns the same value as the most recent :py:func:`next` call (if
        any).

        May raise :py:exc:`IteratorInvalidError`.
        '''
        self._retrieve(reverse=True)
        return self._cache.popleft()

    seek = remote_method("seek", '''
        Move the iterator to the specified `target`.

        This moves the iterator to the the first key that sorts equal or before
        the specified `target` within the iterator range (`start` and `stop`).
        ''', callback="_clear")


class RawIterator(BaseIterator):
    '''
    The raw iteration API mimics the C++ iterator interface provided by LevelDB.
    See the LevelDB documentation for a detailed description.
    '''

    def key(self):
        '''
        Return the current key.

        May raise :py:exc:`IteratorInvalidError`.
        '''
        self._retrieve()
        return self._cache[0][0]

    def value(self):
        '''
        Return the current value.

        May raise :py:exc:`IteratorInvalidError`.
        '''
        self._retrieve()
        return self._cache[0][1]

    def item(self):
        '''
        Return the current key and value as a tuple.

        May raise :py:exc:`IteratorInvalidError`.
        '''
        self._retrieve()
        return self._cache[0]

    valid = remote_method("valid", '''
        Check whether the iterator is currently valid.
        ''', callback="_clear")
    seek_to_first = remote_method("seek_to_first", '''
        Seek to the first key (if any).
        ''', callback="_clear")
    seek_to_last = remote_method("seek_to_last", '''
        Seek to the last key (if any).
        ''', callback="_clear")


class Iterator(BaseIterator):
    seek_to_start = remote_method("seek_to_start", '''
        Move the iterator to the start key (or the begin).

        This "rewinds" the iterator, so that it is in the same state as when first
        created. This means calling :py:func:`next` afterwards will return the
        first entry.
        ''', callback="_clear")
    seek_to_stop = remote_method("seek_to_stop", '''
        Move the iterator to the stop key (or the end).

        This "fast-forwards" the iterator past the end. After this call the
        iterator is exhausted, which means a call to :py:func:`next` raises
        StopIteration, but :py:meth:`~Iterator.prev` will work.
        ''', callback="_clear")


class WriteBatch(RemoteObject):
    '''
    Write batch for batch put/delete operations

    Instances of this class can be used as context managers (Python's ``with``
    block). When the ``with`` block terminates, the write batch will
    automatically write itself to the database without an explicit call to
    :py:meth:`WriteBatch.write`::

        with db.write_batch() as b:
            b.put(b'key', b'value')

    The `transaction` argument to :py:meth:`DB.write_batch` specifies whether the
    batch should be written after an exception occurred in the ``with`` block. By
    default, the batch is written (this is like a ``try`` statement with a
    ``finally`` clause), but if transaction mode is enabled`, the batch will be
    discarded (this is like a ``try`` statement with an ``else`` clause).

    Note: methods on a :py:class:`WriteBatch` do not take a `sync` argument; this
    flag can be specified for the complete write batch when it is created using
    :py:meth:`DB.write_batch`.

    Do not instantiate directly; use :py:meth:`DB.write_batch` instead.

    See the descriptions for :cpp:class:`WriteBatch` and :cpp:func:`DB::Write` in
    the LevelDB C++ API for more information.
    '''
    def __enter__(self):
        return self

    def __exit__(exc_type, exc_val, exc_tb):
        self.close()

    put = remote_method("put", '''
        Set a value for the specified key.

        This is like :py:meth:`DB.put`, but operates on the write batch instead.
        ''')
    delete = remote_method("delete", '''
        Delete the key/value pair for the specified key.

        This is like :py:meth:`DB.delete`, but operates on the write batch
        instead.
        ''')
    clear = remote_method("clear", '''
        Clear the batch.

        This discards all updates buffered in this write batch.
        ''')
    write = remote_method("write", '''
        Write the batch to the associated database. If you use the write batch as
        a context manager (in a ``with`` block), this method will be invoked
        automatically.)
        ''')


class PrefixDB(RemoteObject):
    '''
    A :py:class:`DB`-like object that transparently prefixes all database keys.

    Do not instantiate directly; use :py:meth:`DB.prefixed_db` instead.
    '''
    @property
    def prefix(self):
        '''
        The prefix used by this :py:class:`PrefixedDB`.
        '''
        pass

    @property
    def db(self):
        '''
        The underlying :py:class:`DB` instance.
        '''
        pass

    def __init__(self, rotype, roid, command):
        RemoteObject.__init__(rotype, roid, command)

    get = remote_method("get", '''
        See :py:meth:`DB.get`.
        ''')
    put = remote_method("put", '''
        See :py:meth:`DB.put`.
        ''')
    delete = remote_method("delete", '''
        See :py:meth:`DB.delete`.
        ''')
    write_batch = remote_method("write_batch", '''
        See :py:meth:`DB.write_batch`.
        ''')
    iterator = remote_method("iterator", '''
        See :py:meth:`DB.iterator`.
        ''')
    snapshot = remote_method("snapshot", '''
        See :py:meth:`DB.iterator`.
        ''')
    prefixed_db = remote_method("prefixed_db", '''
        Create another :py:class:`PrefixedDB` instance with an additional key
        prefix, which will be appended to the prefix used by this
        :py:class:`PrefixedDB` instance.

        See :py:meth:`DB.prefixed_db`.
        ''')


class CommandProtocol(object):
    '''
    LevelDB-server protocol implementation.

    Based on a zmq socket pool for concurrency, compatible with gevent.

    :cvar dict default_codes: dictionary of response typecode handling. Values can be either callables or basestrings refering to instance methods.
    :cvar default_remote_types: dictionary of remote object constructors.  Values can be either callables or basestrings refering to instance methods.

    :var basestring database: server database
    :var zmq.Context context: zmq context for sockets
    :var basestring address: zmq server address
    :var list sockets: list of zmq.sockets
    :var Queue sockque: queue (from gevent if green) of sockets
    :var dict codes: dict of response handlers.
    :var dict remote_types: dict of remote object constructors.
    '''
    default_codes = {
        "\0": "serialized",
        "\1": "exception",
        "\2": "remote_object",
        }

    default_remote_types = {
        "Iterator": Iterator,
        "RawIterator": RawIterator,
        }

    def __init__(self, pool_size, green, address, database, timeout=-1):
        '''
        :param int pool_size: number of sockets aka number of concurrent requests
        :param bool green: wether use gevent friendly socket and queue
        :param basestring host: zmq address to connect to
        :param basestring database: server database name
        :param float timeout: timeout in seconds for requests
        '''
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
        self.context.setsockopt(zmq.RCVTIMEO, max(int(timeout*1000), -1))
        self.context.setsockopt(zmq.SNDTIMEO, max(int(timeout*1000), -1))

        self.address = address
        self.sockets = [self._socket(zmq.DEALER) for _ in xrange(pool_size)]
        self.sockque = QueueType()
        self.sockque.queue.extend(self.sockets)

        self.codes = {
            k: getattr(self, v) if isinstance(v, basestring) else v
            for k, v in self.default_codes.iteritems()
            }

        self.remote_types = {
            k: getattr(self, v) if isinstance(v, basestring) else v
            for k, v in self.default_remote_types.iteritems()
            }

        self.remote_type = RemoteObject

    def _socket(self, type):
        '''
        Initialize socket for current context
        '''
        socket = self.context.socket(type)
        socket.connect(self.address)
        return socket

    def close(self):
        '''
        Close all sockets and terminate zmq context.
        '''
        for socket in self.sockets:
            socket.close()
        self.context.term()

    def command(self, op, args=(), kwargs={}):
        '''
        Run given operation in server.
        :param basestring op: operation name
        :param iterable args: operation arguments
        :param dict kwargs: operation keyword arguments
        '''
        socket = self.sockque.get()
        try:
            sargs = msgpack.dumps((args, kwargs))
            socket.send_multipart((self.database, op, sargs))
            if kwargs.get("wait", True):
                data_type, data = socket.recv_multipart()
                return self.codes.get(data_type, self.default)(data, (self, op, args, kwargs))
        finally:
            self.sockque.put(socket)

    def serialized(self, data, command):
        '''
        Convert data sent by server to python objects.

        :param zmq.Socket socket: unused
        :param basestring data: data received from server
        :param command:
        :return: unserialized data
        '''
        return msgpack.loads(data)

    def exception(self, data, command):
        '''
        Raises exception sent by server.

        :param zmq.Socket socket: unused
        :param basestring data: data received from server
        :param command:
        :raises: if raised by server and found in module or builtins
        :raises Exception: if raised by server and not in module or builtins
        '''
        exc_type, exc_args = msgpack.loads(data)
        for scope in (globals(), __builtin__.__dict__):
            if exc_type in scope:
                raise scope[exc_type](*exc_args)
        raise Exception(*exc_args)

    def remote_object(self, data, command):
        '''
        Yield server results, asynchronously from server.

        :param zmq.Socket socket: socket will be used for subsequent requests
        :param basestring data: serialized (rotype, roid) tuple send by server
        :param command:
        :param dict kwargs: operation keyword arguments, bulksize key is used
        '''
        rotype, roid = msgpack.loads(data)
        return self.remote_types.get(rotype, self.remote_type)(rotype, roid, command)

    def default(self, data, command):
        '''
        Send error to logger as this function is reached when no suitable
        handler is found for server data.

        :param zmq.Socket socket: unused
        :param basestring data: data received from server
        :param command:
        '''
        logger.error("Could not parse server message %r" % data)


class Connection(object):
    '''
    Creates a connection to server.

    Note all method receives a **wait** keyword argument, which default
    value depends on return type (False in methods which returns always None),
    which defines whether client should wait for server or not (sync or async
    request).
    '''
    def __init__(self, address, database, timeout=2, green=False, bulksize=10,
                 poolsize=10):
        '''
        Initialize client for given address and database.

        :param basestring address: server zmq address
        :param basestring database: database name
        :param float timeout: send and receive socket timeout in seconds
        :param bool green: Use ZeroMQ 'green' (greenlet friendly) implementation.
        :param int bulksize: number of results will be cached by some remote objects methods
        :param int poolsize: number of concurrent requests
        '''
        self.bulksize = 10
        self.protocol = CommandProtocol(poolsize, green, address, database, timeout)

    def command(self, op, args, kwargs):
        '''
        Run command on server.

        :param basestring op: operation
        :param iterable args: arguments
        :param dict kwargs: keyword arguments
        '''
        return self.protocol.command(op, args, kwargs)

    def iterator(self, *args, **kwargs):
        '''
        Create a new :py:class:`Iterator` instance for this database.

        All arguments are optional, and not all arguments can be used together,
        because some combinations make no sense. In particular:

        * `start` and `stop` cannot be used if a `prefix` is specified.
        * `include_start` and `include_stop` are only used if `start` and `stop`
        are specified.

        Note: due to the whay the `prefix` support is implemented, this feature
        only works reliably when the default DB comparator is used.

        See the :py:class:`Iterator` API for more information about iterators.

        :param bool reverse: whether the iterator should iterate in reverse order
        :param bytes start: the start key (inclusive by default) of the iterator
                          range
        :param bytes stop: the stop key (exclusive by default) of the iterator
                         range
        :param bool include_start: whether to include the start key in the range
        :param bool include_stop: whether to include the stop key in the range
        :param bytes prefix: prefix that all keys in the the range must have
        :param bool include_key: whether to include keys in the returned data
        :param bool include_value: whether to include values in the returned data
        :param bool verify_checksums: whether to verify checksums
        :param bool fill_cache: whether to fill the cache
        :return: new :py:class:`Iterator` instance
        :rtype: :py:class:`Iterator`
        '''
        kwargs.setdefault("bulksize", self.bulksize)
        return self.command("iterator", args, kwargs)

    def raw_iterator(self, *args, **kwargs):
        '''
        Create a new :py:class:`RawIterator` instance for this database.

        See the :py:class:`RawIterator` API for more information.
        '''
        kwargs.setdefault("bulksize", self.bulksize)
        return self.command("raw_iterator", args, kwargs)

    get = remote_method("get", '''
        Get the value for the specified key, or `default` if no value was set.

        See the description for :cpp:func:`DB::Get` in the LevelDB C++ API for
        more information.

        :param bytes key: key to retrieve
        :param default: default value if key is not found
        :param bool verify_checksums: whether to verify checksums
        :param bool fill_cache: whether to fill the cache
        :return: value for the specified key, or `None` if not found
        :rtype: bytes
        ''')
    put = remote_method("put", '''
        Set a value for the specified key.

        See the description for :cpp:func:`DB::Put` in the LevelDB C++ API for
        more information.

        :param bytes key: key to set
        :param bytes value: value to set
        :param bool sync: whether to use synchronous writes
        ''', wait=False)
    delete = remote_method("delete", '''
        Delete the key/value pair for the specified key.

        See the description for :cpp:func:`DB::Delete` in the LevelDB C++ API for
        more information.

        :param bytes key: key to delete
        :param bool sync: whether to use synchronous writes
        ''', wait=False)
    write_batch = remote_method("write_batch", '''
        Create a new :py:class:`WriteBatch` instance for this database.

        See the :py:class:`WriteBatch` API for more information.

        Note that this method does not write a batch to the database; it only
        creates a new write batch instance.

        :param bool transaction: whether to enable transaction-like behaviour when
                               the batch is used in a ``with`` block
        :param bool sync: whether to use synchronous writes
        :return: new :py:class:`WriteBatch` instance
        :rtype: :py:class:`WriteBatch`
        ''', wait=False)
    snapshot = remote_method("snapshot", '''
        Create a new :py:class:`Snapshot` instance for this database.

        See the :py:class:`Snapshot` API for more information.
        ''')
    get_property = remote_method("get_poperty", '''
        Get the specified property from LevelDB.

        This returns the property value or `None` if no value is available.
        Example property name: ``b'leveldb.stats'``.

        See the description for :cpp:func:`DB::GetProperty` in the LevelDB C++ API
        for more information.

        :param bytes name: name of the property
        :return: property value or `None`
        :rtype: bytes
        ''')
    compact_range = remote_method("compact_range", '''
        Compact underlying storage for the specified key range.

        See the description for :cpp:func:`DB::CompactRange` in the LevelDB C++
        API for more information.

        :param bytes start: start key of range to compact (optional)
        :param bytes stop: stop key of range to compact (optional)
        ''', wait=False)
    approximate_size = remote_method("approximate_size", '''
        Return the approximate file system size for the specified range.

        See the description for :cpp:func:`DB::GetApproximateSizes` in the LevelDB
        C++ API for more information.

        :param bytes start: start key of the range
        :param bytes stop: stop key of the range
        :return: approximate size
        :rtype: int
        ''')
    approximate_sizes = remote_method("approximate_sizes", '''
        Return the approximate file system sizes for the specified ranges.

        This method takes a variable number of arguments. Each argument denotes a
        range as a `(start, stop)` tuple, where `start` and `stop` are both byte
        strings. Example::

        db.approximate_sizes(
            (b'a-key', b'other-key'),
            (b'some-other-key', b'yet-another-key'))

        See the description for :cpp:func:`DB::GetApproximateSizes` in the LevelDB
        C++ API for more information.

        :param ranges: variable number of `(start, stop`) tuples
        :return: approximate sizes for the specified ranges
        :rtype: list
        ''')
    prefixed_db = remote_method("prefixed_db", '''
        Return a new :py:class:`PrefixedDB` instance for this database.

        See the :py:class:`PrefixedDB` API for more information.

        :param bytes prefix: prefix to use
        :return: new :py:class:`PrefixedDB` instance
        :rtype: :py:class:`PrefixedDB`
        ''')

    def close(self):
        '''
        Disconnect from server.
        '''
        self.protocol.close()

    def __del__(self):
        self.close()


logger = logging.getLogger(__name__)
