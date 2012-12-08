import unittest

from database import leveldb


class TestLeveldb(unittest.TestCase):
    def setUp(self):
        self.db = leveldb()

    def test_put_and_get(self):
        self.db.put("test", "value")
        self.assertEqual("value", self.db.get("test"))

    def test_delete(self):
        self.db.put("test", "value")
        self.assertEqual("value", self.db.get("test"))

        self.db.delete("test")
        self.assertEqual("", self.db.get("test"))

    def test_range(self):
        data_range = [("key-{}".format(i), i) for i in range(10)]

        # put everything into database
        [self.db.put(key, value) for key, value in data_range]

        # iterate trhought results and compare
        for n, item in enumerate(self.db.range(data_range[5:])):
            name, value = item

            self.assertEqual(name, data_range[n][0])
            self.assertEqual(value, unicode(data_range[n][1]))
