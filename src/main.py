# Autor: Patrik Mokruša (xmokrup00)
"""
.. include:: ../README.md
"""
import json
import threading
import time
from state import State
from discovery.join import DiscoveryJoin
from discovery.broadcast import DiscoveryBroadcast
from discovery.dnssd import DiscoveryDNSSD
from sync.all_sync import AllSync
from sync.dht import SyncDHT
from sync.gossip import SyncGossip
import argparse

from sync.mq import MessageQueueSync

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(required=True)

def add_common_args(p):
    p.add_argument('--ip', required=True, type=str, help='Virtual IP address to use for local node')
    p.add_argument('--port', type=int, default=51820, help='Port for the network')
    p.add_argument('--interface', type=str, default='wg0', help='Network interface name')
    p.add_argument('--prefix', type=int, default=24, help='Subnet prefix length for wg interface (e.g., 24 for /24)')
    p.add_argument('--sync-port', type=int, default=6881, help='Port for synchronization service')
    p.add_argument('--change-check-interval', type=int, default=1, help='Interval to check for changes in seconds')
    p.add_argument('--forwarded-port', action='store_true', help='Is the port forwarded?')

join_parser = subparsers.add_parser('join', help='Join an existing network')
join_parser.add_argument('target_host', type=str, help='Target host to join')
add_common_args(join_parser)
join_parser.add_argument('--bootstrap-port', type=int, default=17777, help='Bootstrap port for joining the network')
join_parser.set_defaults(func='join-direct')

def join_direct(args):
    """ Function initialize program in direct join mode. """
    state = State(args.ip, port=args.port, interface=args.interface, prefix=args.prefix)

    dis = DiscoveryJoin(state, None, bootstrap_port=args.bootstrap_port)
    try:
        sync_info = dis.startJoin(args.target_host, sync_port=args.sync_port) # this arg is used for MQ i need to pass it here
    except Exception as e:
        print(f"Error during JOIN: {e}")
        state.disableNetlink()
        exit(1)



    if sync_info["sync-type"] == "DHT":
        sync = SyncDHT(state, seed_node=(sync_info["sync-ip"], sync_info["sync-port"]), port=args.sync_port)
    elif sync_info["sync-type"] == "Gossip":
        sync = SyncGossip(state, seed_node=sync_info["sync-seed"], port=args.sync_port)
    elif sync_info["sync-type"] == "MQ":
        sync = MessageQueueSync(state, seed_node=sync_info["sync-seed"], port=args.sync_port)
    elif sync_info["sync-type"] == "ALL":
        print("Joining network with all synchronization methods enabled...")
        dht_info, gossip_info, mq_info = AllSync.splitInfo(sync_info)
        sync_mq = MessageQueueSync(state, seed_node=mq_info["sync-seed"], port=args.sync_port, interval=args.change_check_interval)
        sync_dht = SyncDHT(state, seed_node=(dht_info["sync-ip"], dht_info["sync-port"]), port=args.sync_port-1, interval=args.change_check_interval)
        sync_gossip = SyncGossip(state, seed_node=gossip_info["sync-seed"], port=args.sync_port+1, interval=args.change_check_interval)
        sync = AllSync(state, [sync_dht, sync_gossip, sync_mq])
        
    return state, sync

broadcast_parser = subparsers.add_parser('broadcast', help='Broadcast to existing network to join')
add_common_args(broadcast_parser)
broadcast_parser.add_argument('--bootstrap-port', type=int, default=18888, help='Bootstrap port for joining the network')
broadcast_parser.set_defaults(func='broadcast_discover')

def broadcast_discover(args):
    """ Function initialize program in broadcast discovery mode. """
    state = State(args.ip, port=args.port, interface=args.interface, prefix=args.prefix)

    dis = DiscoveryBroadcast(state, bootstrap_port=args.bootstrap_port, injected_sync=None)

    try:
        sync_info = dis.startJoin(sync_port=args.sync_port) # this arg is used for MQ i need to pass it here
    except Exception as e:
        print(f"Error during JOIN: {e}")
        state.disableNetlink()
        exit(1)


    if sync_info["sync-type"] == "DHT":
        sync = SyncDHT(state, seed_node=(sync_info["sync-ip"], sync_info["sync-port"]), port=args.sync_port, interval=args.change_check_interval)
    elif sync_info["sync-type"] == "Gossip":
        sync = SyncGossip(state, seed_node=sync_info["sync-seed"], port=args.sync_port, interval=args.change_check_interval)
    elif sync_info["sync-type"] == "MQ":
        sync = MessageQueueSync(state, seed_node=sync_info["sync-seed"], port=args.sync_port, interval=args.change_check_interval)
    elif sync_info["sync-type"] == "ALL":
        print("Joining network with all synchronization methods enabled...")
        dht_info, gossip_info, mq_info = AllSync.splitInfo(sync_info)
        sync_mq = MessageQueueSync(state, seed_node=mq_info["sync-seed"], port=args.sync_port, interval=args.change_check_interval)
        sync_dht = SyncDHT(state, seed_node=(dht_info["sync-ip"], dht_info["sync-port"]), port=args.sync_port-1, interval=args.change_check_interval)
        sync_gossip = SyncGossip(state, seed_node=gossip_info["sync-seed"], port=args.sync_port+1, interval=args.change_check_interval)
        sync = AllSync(state, [sync_dht, sync_gossip, sync_mq])
        
    return state, sync

dnssd_parser = subparsers.add_parser('dnssd', help='join using DNSSD discovery')
add_common_args(dnssd_parser)
dnssd_parser.set_defaults(func='dnssd_discover')

def dnssd_discover(args):
    """ Function initialize program in DNSSD discovery mode. """
    # state = State(args.ip, port=args.port, interface=args.interface)

    dis = DiscoveryDNSSD()

    info = dis.browseServices()
    if info:
        if info["type"] == "JOIN":
            args.bootstrap_port = info["port"]
            args.target_host = info["ip"]
            state, sync = join_direct(args)
        elif info["type"] == "BROADCAST":
            args.bootstrap_port = info["port"]
            state, sync = broadcast_discover(args)
        else:
            print("Unknown service type discovered via DNSSD.")
            exit(1)

    return state, sync
    


create_parser = subparsers.add_parser('create', help='Create a new network')
add_common_args(create_parser)
create_parser.add_argument('--sync', required=True, type=str, help='Synchronization technology (DHT, Gossip, MQ, ALL)')
create_parser.set_defaults(func='create')

def create(args):
    """ Function initialize program in create mode. """
    f_port = None
    if args.forwarded_port:
        f_port = args.port
    state = State(args.ip, port=args.port, interface=args.interface, prefix=args.prefix, forwarded_port=f_port)

    sync = None
    if args.sync == "DHT":
        sync = SyncDHT(state, port=args.sync_port, interval=args.change_check_interval)
    elif args.sync == "Gossip":
        sync = SyncGossip(state, port=args.sync_port, interval=args.change_check_interval)
    elif args.sync == "MQ":
        sync = MessageQueueSync(state, seed_node=None, port=args.sync_port, interval=args.change_check_interval)
    elif args.sync == "ALL":
        print("Creating network with all synchronization methods enabled...")
        sync_mq = MessageQueueSync(state, seed_node=None, port=args.sync_port+2, interval=args.change_check_interval)
        sync_dht = SyncDHT(state, port=args.sync_port, interval=args.change_check_interval)
        sync_gossip = SyncGossip(state, port=args.sync_port+1, interval=args.change_check_interval)
        sync = AllSync(state, [sync_dht, sync_gossip, sync_mq])
    else:
        print("Unsupported synchronization method specified.")
        exit(1)
    
    print("Network created successfully.")
    print(state.get_config())
    
    
    return state, sync


def main():
    """ Main function to parse arguments and start the program in the specified mode. """
    args = parser.parse_args()
    
    if args.func == 'join-direct':
        state, sync = join_direct(args)
    elif args.func == 'create':
        state, sync = create(args)
    elif args.func == 'broadcast_discover':
        state, sync = broadcast_discover(args)
    elif args.func == 'dnssd_discover':
        state, sync = dnssd_discover(args)
    
    run_flag = True

    disc_join = None
    disc_bcast = None
    ad = None

    help_msg = """
Available commands:
- exit : Exit the program
- discover-join : Start accepting discovery join requests (toggle)
- discover-broadcast : Start listening for broadcast requests (toggle)
- advertise : Advertise direct joinability using DNSSD (toggle)
- help : Show this help message
- info : Show current state information
- ping : Ping all peers in the network
- add-allowed-ips : Add an allowed IP to local node for other peers to route through this node
- remove-allowed-ips : Remove an allowed IP from local node
"""
    print(help_msg)
    while run_flag:
        try:
            input_val = input()
        except:
            input_val = "exit"

        if input_val == "exit": # exit or ctrl + c
            if disc_join:
                disc_join.stopAccept()
            if disc_bcast:
                try:
                    disc_bcast.stopAccept()
                except OSError as e:
                    if e.errno != 107:  # Ignore "Transport endpoint is not connected"
                        print(f"Error occurred while stopping discovery: {e}")
                except Exception as e:
                    print(f"Error occurred while stopping discovery: {e}")
            sync.exitSync()
            state.disableNetlink()
            if ad:
                ad.stopAdvertise()
            break

        elif input_val == "help":
            print(help_msg)
        
        elif input_val == "add-allowed-ips":
            print(f"Currently allowed IPs: {state.allowed_ips}")
            ip = input("Enter IP to allow: ")
            state.add_allowed_ip(ip)
            sync.publishChange(state.ip,
                               state.public_key,
                               state.public_ip,
                               state.public_port,
                               allowed_ips=state.allowed_ips)
        
        elif input_val == "remove-allowed-ips":
            print(f"Currently allowed IPs:")
            for i, ip in enumerate(state.allowed_ips):
                print(f"{i}: {ip}")
            index = input("Enter index of IP to remove from allowed list: ")
            if index.isdigit() and 0 < int(index) < len(state.allowed_ips):
                ip = state.allowed_ips[int(index)]
                state.remove_allowed_ip(ip)
                sync.publishChange(state.ip,
                                   state.public_key,
                                   state.public_ip,
                                   state.public_port,
                                   allowed_ips=state.allowed_ips)
            else:
                print("Cannot remove initial IP or invalid index.")
                
    
        elif input_val == "advertise" or input_val == "ad":
            if ad:
                print("Stopping previous advertisement...")
                ad.stopAdvertise()
                ad = None
            else:
                ad = DiscoveryDNSSD(state)

                if disc_join:
                    ad.startAdvertise(disc_join)
                if disc_bcast:
                    ad.startAdvertise(disc_bcast)
                if not disc_join and not disc_bcast:
                    print("Nothing to advertise.")
                    ad = None
            
        elif input_val == "info":
            print(state.get_config())
            print(f"state.public_ip: {state.public_ip} state.public_port: {state.public_port}")
            print(f"state.peers: {state.peers}")
            print(json.dumps(sync.getInfo(), indent=2))

        elif input_val == "discover-join" or input_val == "dis-join":
            try:
                if disc_join:
                    print("Stopping previous discovery join...")
                    disc_join.stopAccept()
                    disc_join = None
                    continue
            except Exception as e:
                print(f"Failed to start join: {e}")
                continue

            port = input("Enter port for discovery join (default 17777): ")
            if port == "":
                port = 17777
            else:
                port = int(port)
            disc_join = DiscoveryJoin(state, sync, bootstrap_port=port)
            disc_join.startAccept()

        elif input_val == "discover-broadcast" or input_val == "dis-bcast":
            try:
                if disc_bcast:
                    print("Stopping previous discovery broadcast...")
                    disc_bcast.stopAccept()
                    disc_bcast = None
                    continue
            except Exception as e:
                print(f"Failed to start broadcast join: {e}")
                continue

            port = input("Enter port for discovery broadcast (default 18888): ")
            if port == "":
                port = 18888
            else:
                port = int(port)
            disc_bcast = DiscoveryBroadcast(state, sync, bootstrap_port=port)
            try:
                disc_bcast.startAccept()
            except Exception as e:
                print(f"Error in discovery broadcast: {e}")   

        elif input_val == "wait":
            # testing feature
            seconds = input("seconds to wait: ")
            time.sleep(int(seconds))
        elif input_val == "hang":
            while True:
                time.sleep(1)
        elif input_val == "ping":
            state.ping_all_peers()

if __name__ == "__main__":
    main()
