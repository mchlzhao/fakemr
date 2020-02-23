# PyMR

A MapReduce framework implemented in Python 3, leveraging the [multiprocessing](https://docs.python.org/3.8/library/multiprocessing.html "Python multiprocessing documentation") library to run map and reduce tasks in parallel on multiple CPU cores. Users can learn about and experiment with the framework by defining their own map and reduce functions.

There are currently two sample programs demonstrating how to use the PyMR framework:
* [Index inverter](https://github.com/mchlzhao/pymr/blob/master/index_inverter.py)
* [Word counter](https://github.com/mchlzhao/pymr/blob/master/word_counter.py)

The design of the system was inspired by Google's [MapReduce research paper](https://research.google/pubs/pub62/) (Dean & Ghemawat, 2004).

License
==
PyMR is distributed under the terms of the MIT license. See LICENSE for details.
