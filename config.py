import os, traceback
import redis

REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379
REDIS_PREFIX = 'bqueryd_'

BROKER_PORT = 14334
VENTILATOR_PORT = 14335
SINK_PORT = 14336
RPC_PORT = 14337

DATA_FILE_EXTENSION = '.bcolz'
SHARD_DATA_FILE_EXTENSION = '.bcolzs'

try:
    if os.path.exists('config_local.py'):
        from config_local import *
except:
    traceback.print_exc()

REDIS = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

def get(key, default=None):
    tmp = REDIS.get(REDIS_PREFIX+key)
    return tmp or default


def set(key, value):
    REDIS.set(REDIS_PREFIX+key, value)


def append(key, value):
    'Append a value to a list on key'
    REDIS.rpush(REDIS_PREFIX+key, value)


def show_all():
    'Show all settings related to ' + REDIS_PREFIX
    pass