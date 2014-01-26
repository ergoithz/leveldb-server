#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import signal
import unittest
import sys
import os
import subprocess
import gevent

import server
import client

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
        [self.db.put(key, value) for key, value in data_range]

        # iterate trhought results and compare
        for pos, (name, value) in enumerate(self.db.range(data_range[5:]), 4):
            self.assertEqual(name, data_range[pos][0])
            self.assertEqual(value, data_range[pos][1])


def benchmark():
    import timeit
    port = 5147
    os.system("fuser -k -n tcp %d" % port)
    address = "tcp://127.0.0.1:%d" % port
    dbname = "test.db"
    popen = subprocess.Popen((sys.executable, server.__file__, address, dbname))
    db = client.LevelDB(address, dbname)

    elements = 1000000
    print "Initialize greenlets..."
    greenlets = [
        gevent.spawn(db.put, "key-%d" % i, "value-%d" % i)
        for i in xrange(elements)
        ]
    print "Joining..."
    print timeit.timeit(lambda: gevent.joinall(greenlets), number=1)

    popen.send_signal(signal.SIGTERM)
    popen.wait()


if __name__ == '__main__':
    unittest.main()
