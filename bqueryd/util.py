import netifaces
import zmq
import random
import os
import tempfile
import zipfile
import binascii
import time
import sys

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
        zip_info = ''.join(str(zipinfoi.CRC) for zipinfoi in  myzip.infolist())
        checksum = hex(binascii.crc32(zip_info) & 0xffffffff)

    return zip_filename, checksum

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

def show_busy_downloads(info_data):
    all_downloads = info_data['downloads'].copy()
    for x in info_data.get('others', {}).values():
        all_downloads.update(x.get('downloads'))
    for ticket, x in all_downloads.items():
        print ticket, time.ctime(x['created'])
        for filename, nodelist in x['progress'].items():
            print filename
            for node, x in nodelist.items():
                print '   ', node,
                if not x.get('done'):
                    if 'progress' in x:
                        print int(float(x['progress']) / x.get('size', x['progress']) * 100), '%'
                    else:
                        print x
                sys.stdout.write('\n')
