import netifaces
import zmq
import random

def get_my_ip():
    eth_interfaces = sorted([ifname for ifname in netifaces.interfaces() if ifname.startswith('eth')])
    if len(eth_interfaces) < 1:
        ifname = 'lo'
    else:
        ifname = eth_interfaces[-1]
    for x in netifaces.ifaddresses(ifname)[netifaces.AF_INET]:
        # Return first addr found
        return x['addr']

def bind_to_random_port(socket, addr, min_port=49152, max_port=65536, max_tries=100):
    "We can't just use the zmq.Socket.bind_to_random_port, as we wan't to set the identity before binding"
    for i in range(max_tries):
        try:
            port = random.randrange(min_port, max_port)
            socket.identity = '%s:%s' % (addr, port)
            socket.bind('tcp://*:%s' % port)
            #socket.bind('%s:%s' % (addr, port))
        except zmq.ZMQError as exception:
            en = exception.errno
            if en == zmq.EADDRINUSE:
                continue
            else:
                raise
        else:
            return socket.identity
    raise zmq.ZMQBindError("Could not bind socket to random port.")
