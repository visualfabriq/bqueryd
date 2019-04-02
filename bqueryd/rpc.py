import binascii
import json
import logging
import os
import random
import tarfile
import tempfile
import time
import traceback
from tarfile import TarFile, TarError

import boto3
import redis
import smart_open
import zmq

import bqueryd
from bqueryd.messages import msg_factory, RPCMessage, ErrorMessage

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
import glob
import pandas as pd
from bquery import ctable
from bqueryd.tool import rm_file_or_dir


class RPCError(Exception):
    """Base class for exceptions in this module."""
    pass


class RPC(object):

    def __init__(self, address=None, timeout=120, redis_url='redis://127.0.0.1:6379/0', loglevel=logging.INFO,
                 retries=3):
        self.logger = bqueryd.logger.getChild('rpc')
        self.logger.setLevel(loglevel)
        self.context = zmq.Context()
        self.redis_url = redis_url
        redis_server = redis.from_url(redis_url)
        self.retries = retries
        self.timeout = timeout
        self.identity = binascii.hexlify(os.urandom(8))

        if not address:
            # Bind to a random controller
            controllers = list(redis_server.smembers(bqueryd.REDIS_SET_KEY))
            if len(controllers) < 1:
                raise Exception('No Controllers found in Redis set: ' + bqueryd.REDIS_SET_KEY)
            random.shuffle(controllers)
        else:
            controllers = [address]
        self.controllers = controllers
        self.connect_socket()

    def connect_socket(self):
        reply = None
        for c in self.controllers:
            self.logger.debug('Establishing socket connection to %s', c)
            tmp_sock = self.context.socket(zmq.REQ)
            tmp_sock.setsockopt(zmq.RCVTIMEO, 2000)
            tmp_sock.setsockopt(zmq.LINGER, 0)
            tmp_sock.identity = self.identity
            tmp_sock.connect(c)
            # first ping the controller to see if it responds at all
            msg = RPCMessage({'payload': 'ping'})
            tmp_sock.send_json(msg)
            try:
                reply = msg_factory(tmp_sock.recv_json())
                self.address = c
                break
            except:
                logging.exception("Unable to connect to %s", c)
                continue
        if reply:
            # Now set the timeout to the actual requested
            self.logger.debug("Connection OK, setting network timeout to %s milliseconds", self.timeout * 1000)
            self.controller = tmp_sock
            self.controller.setsockopt(zmq.RCVTIMEO, self.timeout * 1000)
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
            rep = None
            for x in range(self.retries):
                try:
                    self.controller.send_json(msg)
                    rep = self.controller.recv()
                    break
                except Exception, e:
                    self.controller.close()
                    self.logger.critical(e)
                    if x == self.retries:
                        raise e
                    else:
                        self.logger.debug("Error, retrying %s" % (x + 1))
                        self.connect_socket()
                        pass
            if not rep:
                raise RPCError("No response from DQE, retries %s exceeded" % self.retries)
            try:
                # The results returned from controller is a tarfile with all the results, convert it to a Dataframe
                if name == 'groupby':
                    _, groupby_col_list, agg_list, where_terms_list = args[0], args[1], args[2], args[3]
                    result = self.uncompress_groupby_to_df(rep, groupby_col_list, agg_list, where_terms_list,
                                                           aggregate=kwargs.get('aggregate', False))
                else:
                    rep = msg_factory(json.loads(rep))
                    result = rep.get_from_binary('result')
            except (ValueError, TypeError):
                self.logger.exception('Could not use RPC method: {}/{}'.format(name, rep))
                result = rep
            if isinstance(rep, ErrorMessage):
                raise RPCError(rep.get('payload'))
            stop_time = time.time()
            self.last_call_duration = stop_time - start_time
            return result

        return _rpc

    def distribute(self, filenames, bucket):
        """
        Upload a local filename to the specified S3 bucket, and then issue a download command
        using the hash of the file.
        """
        for filename in filenames:
            if filename[0] != '/':
                filepath = os.path.join(bqueryd.DEFAULT_DATA_DIR, filename)
            else:
                filepath = filename
            if not os.path.exists(filepath):
                raise RPCError('Filename %s not found' % filepath)

            # Try to compress the whole bcolz direcory into a single zipfile
            tmpzip_filename, signature = bqueryd.util.zip_to_file(filepath, bqueryd.INCOMING)

            session = boto3.Session()
            credentials = session.get_credentials()
            if not credentials:
                raise ValueError('Missing S3 credentials')

            credentials = credentials.get_frozen_credentials()
            access_key = credentials.access_key
            secret_key = credentials.secret_key

            key = 's3://{}:{}@{}/{}'.format(access_key, secret_key, self.bucket, filename)

            # Use smart_open to stream the file into S3 as the files can get very large
            with smart_open.smart_open(key, mode='wb') as fout:
                fout.write(open(tmpzip_filename).read())

            os.remove(tmpzip_filename)

        signature = self.download(filenames=filenames, bucket=bucket)

        return signature

    def uncompress_groupby_to_df(self, result_tar, groupby_col_list, agg_list, where_terms_list, aggregate=False):
        # uncompress result retured by the groupby and convert it to a Pandas DataFrame
        try:
            tar_file = TarFile(fileobj=StringIO(result_tar))
            tmp_dir = tempfile.mkdtemp(prefix='tar_dir_')
            tar_file.extractall(tmp_dir)
        except TarError:
            self.logger.exception("Could not create/extract tar.")
            raise ValueError(result_tar)
        del result_tar
        del tar_file

        ct = None

        # now untar and aggregate the individual shard results
        for i, sub_tar in enumerate(glob.glob(os.path.join(tmp_dir, '*'))):
            new_dir = os.path.join(tmp_dir, 'bcolz_' + str(i))
            rm_file_or_dir(new_dir)
            with tarfile.open(sub_tar, mode='r') as tar_file:
                tar_file.extractall(new_dir)
            # rm_file_or_dir(sub_tar)
            ctable_dir = glob.glob(os.path.join(new_dir, '*'))[0]
            new_ct = ctable(rootdir=ctable_dir, mode='a')
            if i == 0:
                ct = new_ct
            else:
                ct.append(new_ct)

        # aggregate by groupby parameters
        if ct is None:
            result_df = pd.DataFrame()
        elif aggregate:
            new_dir = os.path.join(tmp_dir, 'end_result')
            rm_file_or_dir(new_dir)
            # we can only sum now
            new_agg_list = [[x[2], 'sum', x[2]] for x in agg_list]
            result_ctable = ct.groupby(groupby_col_list, new_agg_list, rootdir=new_dir)
            result_df = result_ctable.todataframe()
        else:
            result_df = ct.todataframe()

        rm_file_or_dir(tmp_dir)

        return result_df

    def get_download_data(self):
        redis_server = redis.from_url(self.redis_url)
        tickets = set(redis_server.keys(bqueryd.REDIS_TICKET_KEY_PREFIX + '*'))
        data = {}
        for ticket in tickets:
            tmp = redis_server.hgetall(ticket)
            data[ticket] = tmp
        return data

    def downloads(self):
        data = self.get_download_data()
        buf = []
        for k, v in data.items():
            done_count = 0
            for kk, vv in v.items():
                if vv.endswith('_DONE'):
                    done_count += 1
            buf.append((k, '%s/%s' % (done_count, len(v))))
        return buf

    def delete_download(self, ticket):
        redis_server = redis.from_url(self.redis_url)
        tmp = redis_server.hgetall(ticket)
        count = 0
        for k, v in tmp.items():
            count += redis_server.hdel(ticket, k)
        return count
