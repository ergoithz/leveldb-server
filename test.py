#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import itertools
import signal
import unittest
import sys
import os

import gevent
import gevent.subprocess as subprocess

import server
import client

import old_server
import old_client

class TestLevelDB(unittest.TestCase):
    def setUp(self):
        port = 5147
        address = "tcp://127.0.0.1:%d" % port
        dbname = "test.db"
        self.popen = subprocess.Popen((sys.executable, server.__file__, "--verbose", address, dbname))
        self.db = client.LevelDB(address, dbname)

    def tearDown(self):
        self.popen.send_signal(signal.SIGTERM)
        self.popen.wait()

    def test_put_and_get(self):
        self.db.put("test", "value")
        self.assertEqual("value", self.db.get("test"))

    def test_delete(self):
        self.db.put("test", "value")
        self.assertEqual("value", self.db.get("test"))

        self.db.delete("test")
        self.assertRaises(KeyError, self.db.get, "test")

    def test_range(self):
        data_range = [("key-%s" % i, str(i)) for i in range(10)]

        # put everything into database
        for key, value in data_range:
            self.db.put(key, value)

        # iterate through results and compare
        data = itertools.izip(
            self.db.range_iter(data_range[5][0], data_range[-1][0]),
            data_range[5:]
            )
        for (k1, v1), (k2, v2) in data:
            self.assertEqual(k1, k2)
            self.assertEqual(v1, v2)


def benchmark():

    import timeit
    port = 5147
    os.system("fuser -k -n tcp %d" % port)
    address = "tcp://127.0.0.1:%d" % port
    dbname = "test.db"

    print "Current implementation"
    popen = subprocess.Popen((sys.executable, server.__file__, address, dbname))
    gevent.sleep(1) # Wait for server
    db = client.LevelDB(address, dbname)
    elements = 100000
    print "    Initialize %d 'put' greenlets..." % elements
    greenlets = [
        gevent.spawn(db.put, "key-%d" % i, "value-%d" % i)
        for i in xrange(elements)
        ]
    print "    Joining..."
    print "    %f" % timeit.timeit(lambda: gevent.joinall(greenlets), number=1)

    popen.send_signal(signal.SIGTERM)
    popen.wait()

    print "Old implementation"
    os.system("fuser -k -n tcp %d" % port)
    popen = subprocess.Popen((sys.executable, old_server.__file__, "-l", address, "--dbfiles=%s" % dbname))
    gevent.sleep(1)
    db = old_client.leveldb(dbname, address)
    elements = 100000
    print "    Initialize %d 'put' greenlets..." % elements
    greenlets = [
        gevent.spawn(db.put, "key-%d" % i, "value-%d" % i)
        for i in xrange(elements)
        ]
    print "    Joining..."
    print "    %f" % timeit.timeit(lambda: gevent.joinall(greenlets), number=1)

    popen.send_signal(signal.SIGTERM)
    popen.wait()


if __name__ == '__main__':
    unittest.main()
    #benchmark()
