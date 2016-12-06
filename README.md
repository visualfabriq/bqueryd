# bqueryd

Distributed Bquery

Allow sharding of bcolz files and computations to be spread over different nodes on the network.
Use the ZeroMQ Distributed Message library. http://zeromq.org/ for communication.

Firct cut design consists of a QNode that distributes an incoming RPC call over n Worker nodes, using the
Ventilator-Sink pattern.

## Getting started

    virtualenv .
    . ./bin/activate
    pip install redis bquery zmq
    python test.py

