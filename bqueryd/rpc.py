import bqueryd
import zmq
import logging
logger = logging.getLogger('bqueryd RPC')
import time
from bqueryd.messages import msg_factory, RPCMessage, ErrorMessage


class RPCError(Exception):
    """Base class for exceptions in this module."""
    pass


class RPC(object):
    def __init__(self, ip='127.0.0.1', timeout=3600):
        self.context = zmq.Context()
        self.controller = self.context.socket(zmq.REQ)
        self.controller.setsockopt(zmq.RCVTIMEO, timeout*1000)
        self.controller.setsockopt(zmq.LINGER, 0)

        rpc_address = 'tcp://$s:%s' % (ip, bqueryd.RPC_PORT)
        self.controller.connect(rpc_address)
        logger.debug('Will do RPC requests to %s' % rpc_address)

    def __getattr__(self, name):
        def _rpc(*args, **kwargs):
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