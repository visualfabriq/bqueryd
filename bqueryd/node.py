#!/usr/bin/env python
import logging
import sys

import configobj

import bqueryd

config = configobj.ConfigObj('/etc/bqueryd.cfg')
redis_url = config.get('redis_url', 'redis://127.0.0.1:6379/0')


def main(argv=sys.argv):
    if '-vvv' in argv:
        loglevel = logging.DEBUG
    elif '-vv' in argv:
        loglevel = logging.INFO
    elif '-v' in argv:
        loglevel = logging.WARNING
    else:
        loglevel = logging.ERROR

    data_dir = bqueryd.DEFAULT_DATA_DIR
    for arg in argv:
        if arg.startswith('--data_dir='):
            data_dir = arg[11:]

    if 'controller' in argv:
        bqueryd.ControllerNode(redis_url=redis_url, loglevel=loglevel).go()
    elif 'worker' in argv:
        bqueryd.WorkerNode(redis_url=redis_url, loglevel=loglevel, data_dir=data_dir).go()
    elif 'downloader' in argv:
        bqueryd.DownloaderNode(redis_url=redis_url, loglevel=loglevel).go()
    elif 'movebcolz' in argv:
        bqueryd.MoveBcolzNode(redis_url=redis_url, loglevel=loglevel).go()
    else:
        if len(argv) > 1 and argv[1].startswith('tcp:'):
            rpc = bqueryd.RPC(address=argv[1], redis_url=redis_url, loglevel=loglevel)
        else:
            rpc = bqueryd.RPC(redis_url=redis_url, loglevel=loglevel)
        import IPython
        IPython.embed()


if __name__ == '__main__':
    main()
