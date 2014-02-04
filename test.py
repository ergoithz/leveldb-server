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
        self.db.put("test", "value", sync=True)
        self.assertEqual("value", self.db.get("test"))

    def test_delete(self):
        self.db.put("test", "value", sync=True)
        self.assertEqual("value", self.db.get("test"))

        self.db.delete("test", sync=True)
        self.assertEqual(None, self.db.get("test"))

    def test_iterator(self):
        data_range = [("key-%05d" % i, str(i)) for i in range(25)]

        # put everything into database
        for key, value in data_range:
            self.db.put(key, value, sync=True)

        # iterate through results and compare
        data = itertools.izip(
            self.db.iterator(False, data_range[-5][0], data_range[-1][0]),
            data_range[-5:]
            )
        for (k1, v1), (k2, v2) in data:
            self.assertEqual(k1, k2)
            self.assertEqual(v1, v2)


def benchmark():
    import timeit
    port = 5147

    address = "tcp://127.0.0.1:%d" % port
    tmpdb = tempfile.mkdtemp()
    dbname = os.path.basename(tmpdb)
    elements = 10000

    print "Current implementation"
    popen = subprocess.Popen((sys.executable, server.__file__, address, tmpdb))
    gevent.sleep(1) # Wait for server
    db = client.LevelDB(address, dbname, green=True)

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

    import old_server
    import old_client

    print "Old implementation"
    popen = subprocess.Popen((sys.executable, old_server.__file__, "-l", address, "--dbfiles=%s" % tmpdb))
    gevent.sleep(1)
    db = old_client.leveldb(dbname, address)
    print "    Initialize %d 'put' greenlets..." % elements
    greenlets = [
        gevent.spawn(db.put, "key-%d" % i, "value-%d" % i)
        for i in xrange(elements)
        ]
    print "    Joining..."
    print "    %f" % timeit.timeit(lambda: gevent.joinall(greenlets), number=1)
    '''
    # DISABLED: OLD IMPLEMENTATION FAILS
    print "    Initialize %d 'get' greenlets..." % elements
    greenlets = [
        gevent.spawn(db.get, "key-%d" % i)
        for i in xrange(elements)
        ]
    print "    Joining..."
    print "    %f" % timeit.timeit(lambda: gevent.joinall(greenlets), number=1)
    '''
    popen.send_signal(signal.SIGTERM)
    popen.wait()
    shutil.rmtree(tmpdb)

if __name__ == '__main__':
    unittest.main()
    #benchmark()
