import logging
import zmq
import time
import redis
import random
import bqueryd
from bqueryd.messages import msg_factory, RPCMessage, ErrorMessage
import traceback

class RPCError(Exception):
    """Base class for exceptions in this module."""
    pass


class RPC(object):
    def __init__(self, address=None, timeout=3600, redis_url='redis://127.0.0.1:6379/0', loglevel=logging.DEBUG):
        self.logger = bqueryd.logger.getChild('rpc')
        self.logger.setLevel(loglevel)
        self.context = zmq.Context()
        redis_server = redis.from_url(redis_url)

        if not address:
            # Bind to a random controller
            controllers = list(redis_server.smembers(bqueryd.REDIS_SET_KEY))
            if len(controllers) < 1:
                raise Exception('No Controllers found in Redis set: ' + bqueryd.REDIS_SET_KEY)
            random.shuffle(controllers)
        else:
            controllers = [address]

        reply = None
        for c in controllers:
            self.logger.debug('Trying RPC to %s' % c)
            tmp_sock = self.context.socket(zmq.REQ)
            tmp_sock.setsockopt(zmq.RCVTIMEO, 750)
            tmp_sock.setsockopt(zmq.LINGER, 0)
            tmp_sock.connect(c)
            # first ping the controller to see if it respnds at all
            msg = RPCMessage({'payload': 'ping'})
            tmp_sock.send_json(msg)
            try:
                reply = msg_factory(tmp_sock.recv_json())
                self.address = c
                break
            except:
                traceback.print_exc()
                self.logger.debug('Error on %s, removing it from redis set %s' %(c, bqueryd.REDIS_SET_KEY))
                redis_server.srem(bqueryd.REDIS_SET_KEY, c)
                continue
        if reply:
            # Now set the timeout to the actual requested
            self.controller = tmp_sock
            self.controller.setsockopt(zmq.RCVTIMEO, timeout*1000)
        else:
            raise Exception('No controller connection')


    def __getattr__(self, name):
        def _rpc(*args, **kwargs):
            self.logger.debug('Call %s on %s' % (name, self.address))
            start_time = time.time()
            params = {}
            if args:
                params['args'] = args
            if kwargs:
                params['kwargs'] = kwargs
            # We do not want string args to be converted into unicode by the JSON machinery
            # bquery ctable does not like col names to be unicode for example
            msg = RPCMessage({'payload': name})
            msg.add_as_binary('params', params)
            self.controller.send_json(msg)
            rep = msg_factory(self.controller.recv_json())
            if isinstance(rep, ErrorMessage):
                raise RPCError(rep.get('payload'))
            result = rep.get_from_binary('result')
            stop_time = time.time()
            self.last_call_duration = stop_time - start_time
            return result

        return _rpc