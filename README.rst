
leveldb-server
=============

.. image:: https://travis-ci.org/ergoithz/leveldb-server.png?branch=master   :target: https://travis-ci.org/ergoithz/leveldb-server

 * Async leveldb server and client
 * Storage engine *leveldb* with *plyvel*. https://github.com/wbolster/plyvel
 * Networking library *zeromq*. http://www.zeromq.org/
 * Server based on *gevent* for massive concurrency. http://www.gevent.org/
 * Client compatible with gevent.

License
-------------

New BSD license. Please see license.txt for more details.

Feature
=============

 * Very simple key-value storage
 * Data is sorted by key allowing iteration
 * Data could be automatically compressed
 * Can be run as persistent cache
 * Simple backups (copying a directory)
 * Networking/wiring from **zeromq** messaging library
 * Async gevent-compatible client.
 * Easy polyglot client bindings. See `zmq bindings`_

.. _zmq bindings: http://www.zeromq.org/bindings:_start

.. TODO: Code example once API were stabilized

Usage
-------------

.. code-block:: python

    #!/user/bin/env python
    from leveldb_server.client import Connection

    con = Connection("tcp://127.0.0.1:5147")
    con.set("key", "value")

Dependencies
-------------
(see requirements.txt)

 * gevent
 * pyzmq
 * plyvel

Getting Started
=============

I highly recommends 'virtualenv'. Virtualenv is an script for easy dependency installation and should be used for any
serious project for easing deployment and dependency isolation.

Install dependencies
-------------

1. First you need LevelDB binary and development libraries
    For example, in ubuntu (12.10 or later) just run

.. code-block:: bash

    sudo apt-get install libleveldb1 libleveldb-dev

2. Then create a virtualenv and proccess the requirements file *requirements.txt* included in this project.

.. code-block:: bash

    virtualenv env
    . env/bin/activate
    pip install -r requirements.txt

Using the `client library`_
-------------

.. _client library: https://github.com/ergoithz/leveldb-server/blob/master/client.py

.. code-block:: python

    #!/user/bin/env python
    from leveldb_server import client
    db = client.Connection("tcp://localhost:9010", "testdb")
    db.get("Key")
    db.put("K", "V")
    db.delete("K")

Backups
=============

LevelDB stores database into a single file.

.. code-block:: bash

    cp -rpf /path/to/database /path/to/database_backup

Known issues and work in progress
=============

I'm currently working on (by priority order)

 * Stabilize api and code
 * Async server connection handling, although leveldb does not allow true parallelization.
 * Benchmarking and performance analysis
 * Client timeout
 * Autosharding/replication built on top of ZeroMQ
 * Client libraries for other languages (maybe Haxe)

Thanks
=============

The original guys started and abandoned leveldb-server project, leaving some non-working code on github which inspired me to start this project.

`Wouter Bolsterlee`_, which created the first production-ready LevelDB python wrapper: plyvel_

.. _Wouter Bolsterlee: https://github.com/wbolster
.. _plyvel: https://github.com/wbolster/plyvel

