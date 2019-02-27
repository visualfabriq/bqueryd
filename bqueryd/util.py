import binascii
import netifaces
import os
import random
import shutil
import tempfile
import time
import zipfile

import zmq


def get_my_ip():
    eth_interfaces = sorted(
        [ifname for ifname in netifaces.interfaces() if (ifname.startswith('eth') or ifname.startswith('en'))])
    if len(eth_interfaces) < 1:
        ifname = 'lo'
    else:
        ifname = eth_interfaces[-1]
    for x in netifaces.ifaddresses(ifname)[netifaces.AF_INET]:
        # Return first addr found
        return str(x['addr'])


def bind_to_random_port(socket, addr, min_port=49152, max_port=65536, max_tries=100):
    "We can't just use the zmq.Socket.bind_to_random_port, as we wan't to set the identity before binding"
    for i in range(max_tries):
        try:
            port = random.randrange(min_port, max_port)
            socket.identity = '%s:%s' % (addr, port)
            socket.bind('tcp://*:%s' % port)
            # socket.bind('%s:%s' % (addr, port))
        except zmq.ZMQError as exception:
            en = exception.errno
            if en == zmq.EADDRINUSE:
                continue
            else:
                raise
        else:
            return socket.identity
    raise zmq.ZMQBindError("Could not bind socket to random port.")


def zip_to_file(file_path, destination):
    fd, zip_filename = tempfile.mkstemp(suffix=".zip", dir=destination)
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as myzip:
        if os.path.isdir(file_path):
            abs_src = os.path.abspath(file_path)
            for root, dirs, files in os.walk(file_path):
                for current_file in files:
                    absname = os.path.abspath(os.path.join(root, current_file))
                    arcname = absname[len(abs_src) + 1:]
                    myzip.write(absname, arcname)
        else:
            myzip.write(file_path, file_path)
        zip_info = ''.join(str(zipinfoi.CRC) for zipinfoi in myzip.infolist())
        checksum = hex(binascii.crc32(zip_info) & 0xffffffff)

    return zip_filename, checksum


def rm_file_or_dir(path):
    if os.path.exists(path):
        if os.path.isdir(path):
            if os.path.islink(path):
                os.unlink(path)
            else:
                shutil.rmtree(path)
        else:
            if os.path.islink(path):
                os.unlink(path)
            else:
                os.remove(path)


def tree_checksum(path):
    allfilenames = set()
    for root, dirs, filenames in os.walk(path):
        for filename in filenames:
            allfilenames.add(os.path.join(root, filename))
    buf = ''.join(sorted(allfilenames))
    return hex(binascii.crc32(buf) & 0xffffffff)


###################################################################################################
# Various Utility methods for user-friendly info display

def show_workers(info_data, only_busy=False):
    'For the given info_data dict, show a humand-friendly overview of the current workers'
    nodes = {}
    for w in info_data.get('workers', {}).values():
        nodes.setdefault(w['node'], []).append(w)
    for k, n in nodes.items():
        print k
        for nn in n:
            if only_busy and not nn.get('busy'):
                continue
            print '   ', time.ctime(nn['last_seen']), nn.get('busy')
