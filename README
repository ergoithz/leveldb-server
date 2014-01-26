leveldb-server
=============

* Async leveldb server and client based on zeromq
* Storage engine *"leveldb":http://code.google.com/p/leveldb/*
* Networking library *"zeromq":http://www.zeromq.org/*
* Server based on gevent for massive concurrency.
* Client compatible with gevent.

License
-------------

New BSD license. Please see license.txt for more details.

Feature
=============

* Very simple key-value storage
* Data is sorted by key - allows @range@ queries
* Data is automatically compressed 
* Can act as persistent cache
* Simple backups @cp -rf level.db backup.db@ 
* Networking/wiring from @zeromq@ messaging library - allows many topologies
* Async server for scalability and capacity
* Sync client for easy coding
* Easy polyglot client bindings. See *"zmq bindings":http://www.zeromq.org/bindings:_start*

::
    db.put("k3", "v3")
    db.get("k3")
    # "v3"
    db.range()
    # generator
    list(db.range())
    # '[["k1", "v1"], ["k2", "v2"], ["k3", "v3"]]'
    list(db.range("k1", "k2"))
    # '[["k1", "v1"], ["k2", "v2"]]'
    db.delete('k1')


Will be adding high availability, replication and autosharding using the same zeromq framework. 

Dependencies
-------------
(see requirements.txt)

* gevent
* pyzmq
* leveldb

Getting Started
=============

I highly recommends 'virtualenv'. Virtualenv is an script for easy dependency installation and should be used for any
serious project for easing deployment and dependency isolation.

# Install dependencies
::
    pip install -r requirements.txt

Using the "leveldb-client-py":https://github.com/ergoithz/leveldb-server/blob/master/client.py
-------------

::
    from leveldb_server import client
    db = client.LevelDB()
    db.get("Key")
    db.put("K", "V")
    db.range()
    db.range(start, end)
    db.delete("K")


Backups
=============
::
    cp -rpf level.db backup.db

Known issues and work in progress
=============

Would love your pull requests on
* Benchmarking and performance analysis
* client libraries for other languages
* [issue] zeromq performance issues with 1M+ inserts at a time
* [feature] timeouts in client library
* [feature] support for counters
* [feature] limit support in range queries
* Serializing and seperate threads for get/put/range in leveldb-server
* HA/replication/autosharding and possibly pub-sub for replication

Thanks
=============

Thanks to all the folks who have contributed to all the dependencies. Special thanks to pyzmq/examples/mongo* author for inspiration. 
