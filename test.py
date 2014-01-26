#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import signal
import unittest
import sys
import os
import subprocess

import server
import client

class TestLevelDB(unittest.TestCase):
    def setUp(self):
        port = 5147
        address = "tcp://127.0.0.1:%d" % port
        dbname = "test.db"
        os.system("fuser -k -n tcp %d" % port)
        self.popen = subprocess.Popen((sys.executable, server.__file__, address, dbname))
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
        self.assertEqual(None, self.db.get("test"))

    def test_range(self):
        data_range = [("key-%s" % i, str(i)) for i in range(10)]

        # put everything into database
        [self.db.put(key, value) for key, value in data_range]

        # iterate trhought results and compare
        for pos, (name, value) in enumerate(self.db.range(data_range[5:]), 4):
            self.assertEqual(name, data_range[pos][0])
            self.assertEqual(value, data_range[pos][1])

if __name__ == '__main__':
    unittest.main()