import netifaces

def get_my_ip():
    eth_interfaces = [ifname for ifname in netifaces.interfaces() if ifname.startswith('eth')]
    if len(eth_interfaces) < 1:
        ifname = 'lo'
    else:
        ifname = eth_interfaces[0]
    for x in netifaces.ifaddresses(ifname)[netifaces.AF_INET]:
        # Return first addr found
        return x['addr']
