#!/usr/bin/env python
#Copyright (c) 2011 Fabula Solutions. All rights reserved.
#Use of this source code is governed by a BSD-style license that can be
#found in the license.txt file.

# leveldb client
import zmq
import json
import struct


class leveldb(object):
    """leveldb client"""
    def __init__(self, database=None,
                 host="tcp://127.0.0.1:5147", timeout=3):
        self.host = host
        self.timeout = timeout
        self.connect()
        self.database = database or "level.db"

    def connect(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.XREQ)
        self.socket.connect(self.host)

    def _cmd(self, cmd):
        return "{}:{}".format(self.database, cmd)

    def _decode(self, msg):
        fmt = r"Q"
        size = struct.calcsize(fmt)
        msg = msg[0]

        while msg:
            data_len = struct.unpack(fmt, msg[:size])[0]
            data = msg[size:size + data_len]
            yield data

            msg = msg[size + data_len:]

    def get(self, key):
        """
        ``_decode`` method return generator but there's always **single**
        value of some key, so it's reasonable to cast it into list and return
        first value
        """
        self.socket.send_multipart([self._cmd("get"), json.dumps(key)])
        return list(self._decode(self.socket.recv_multipart()))[0]

    def put(self, key, value):
        self.socket.send_multipart(
            [self._cmd('put'), json.dumps([key, value])])
        return list(self._decode(self.socket.recv_multipart()))[0]

    def delete(self, key):
        self.socket.send_multipart([self._cmd('delete'), json.dumps(key)])
        return list(self._decode(self.socket.recv_multipart()))[0]

    def range(self, start=None, end=None):
        """
        Range return key/value items
        """
        self.socket.send_multipart(
            [self._cmd('range'), json.dumps([start, end])])

        name = None

        for item in self._decode(self.socket.recv_multipart()):
            if not name:
                name = item
                continue

            if name:
                yield name, item
                name = None

    def close(self):
        self.socket.close()
        self.context.term()
