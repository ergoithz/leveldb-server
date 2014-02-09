#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import itertools
import signal
import unittest
import sys
import os
import tempfile
import shutil

import gevent
import gevent.subprocess as subprocess

import server
import client

class TestClient(unittest.TestCase):
    def setUp(self):
        port = 5147
        os.system("fuser -k -n tcp %d" % port)
        address = "tcp://127.0.0.1:%d" % port
        self.tmpdb = tempfile.mkdtemp()
        self.popen = subprocess.Popen(
            (sys.executable, "-u", server.__file__, "--verbose", address, "--create-if-missing", self.tmpdb)
            )
        self.db = client.Connection(address, os.path.basename(self.tmpdb), green=True)

    def tearDown(self):
        self.popen.send_signal(signal.SIGTERM)
        self.popen.wait()
        shutil.rmtree(self.tmpdb)

    def test_put_and_get(self):
        self.db.put("test", "value", wait=True)
        self.assertEqual("value", self.db.get("test"))

    def test_delete(self):
        self.db.put("test", "value", wait=True)
        self.assertEqual("value", self.db.get("test"))

        self.db.delete("test", wait=True)
        self.assertEqual(None, self.db.get("test"))

    def test_iterator(self):
        data_range = [("key-%05d" % i, str(i)) for i in range(100)]
        first_key = data_range[0][0]
        last_key = data_range[-1][0]

        for key, value in data_range:
            self.db.put(key, value, wait=True)

        it = self.db.iterator(False, first_key, last_key, True, True)
        with it:
            zipped = itertools.izip(data_range, it)
            for (key, value), (iter_key, iter_value) in zipped:
                self.assertEqual(key, iter_key)
                self.assertEqual(value, iter_value)

        it = self.db.iterator(True, first_key, last_key, True, True)
        with it:
            zipped = itertools.izip(reversed(data_range), it)
            for (key, value), (iter_key, iter_value) in zipped:
                self.assertEqual(key, iter_key)
                self.assertEqual(value, iter_value)


def benchmark():
    import timeit
    port = 5147

    address = "tcp://127.0.0.1:%d" % port
    tmpdb = tempfile.mkdtemp()
    dbname = os.path.basename(tmpdb)
    elements = 10000

    print "Current implementation"
    popen = subprocess.Popen((sys.executable, server.__file__, address, "--create-if-missing", tmpdb))
    gevent.sleep(1) # Wait for server
    db = client.Connection(address, dbname, green=True)

    print "    Initialize %d 'put' greenlets..." % elements
    greenlets = [
        gevent.spawn(db.put, "key-%d" % i, "value-%d" % i)
        for i in xrange(elements)
        ]
    print "    Joining..."
    print "    %f" % timeit.timeit(lambda: gevent.joinall(greenlets), number=1)
    print "    Initialize %d 'get' greenlets..." % elements
    greenlets = [
        gevent.spawn(db.get, "key-%d" % i)
        for i in xrange(elements)
        ]
    print "    Joining..."
    print "    %f" % timeit.timeit(lambda: gevent.joinall(greenlets), number=1)
    popen.send_signal(signal.SIGTERM)
    popen.wait()
    shutil.rmtree(tmpdb)


if __name__ == '__main__':
    unittest.main()
    #benchmark()
