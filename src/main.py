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
    p.add_argument('--run', action='store_true', help='Run the network after joining/creating')
    p.add_argument('--ip', required=True, type=str, help='Virtual IP address to use')
    p.add_argument('--port', type=int, default=51820, help='Port for the network')
    p.add_argument('--interface', type=str, default='wg0', help='Network interface name')
    p.add_argument('--sync-port', type=int, default=6881, help='Port for synchronization service')
    p.add_argument('--change-check-interval', type=int, default=10, help='Interval to check for changes in seconds')

join_parser = subparsers.add_parser('join', help='Join an existing network')
join_parser.add_argument('target_host', type=str, help='Target host to join')
add_common_args(join_parser)
join_parser.add_argument('--bootstrap-port', type=int, default=17777, help='Bootstrap port for joining the network')
join_parser.set_defaults(func='join-direct')

def join_direct(args):
    state = State(args.ip, port=args.port, interface=args.interface)

    dis = DiscoveryJoin(state, None, bootstrap_port=args.bootstrap_port)
    try:
        sync_info = dis.startJoin(args.target_host, sync_port=args.sync_port) # this arg is used for MQ i need to pass it here
    except Exception as e:
        print(f"Error during JOIN: {e}")
        exit(1)


    state.write_config()
    state.load_config()

    if sync_info["sync-type"] == "DHT":
        sync = SyncDHT(state, seed_node=(sync_info["sync-ip"], sync_info["sync-port"]), port=args.sync_port)
    elif sync_info["sync-type"] == "Gossip":
        sync = SyncGossip(state, seed_node=sync_info["sync-seed"], port=args.sync_port)
    elif sync_info["sync-type"] == "MQ":
        sync = MessageQueueSync(state, seed_node=sync_info["sync-seed"], port=args.sync_port)
    elif sync_info["sync-type"] == "ALL":
        print("Joining network with all synchronization methods enabled...")
        dht_info, gossip_info, mq_info = AllSync.splitInfo(sync_info)
        sync_dht = SyncDHT(state, seed_node=(dht_info["sync-ip"], dht_info["sync-port"]), port=args.sync_port-1)
        sync_gossip = SyncGossip(state, seed_node=gossip_info["sync-seed"], port=args.sync_port+1)
        sync_mq = MessageQueueSync(state, seed_node=mq_info["sync-seed"], port=args.sync_port, interval=args.change_check_interval)
        sync = AllSync(state, [sync_dht, sync_gossip, sync_mq])
        
    return state, sync, args.run

broadcast_parser = subparsers.add_parser('broadcast', help='Broadcast to existing network to join')
add_common_args(broadcast_parser)
broadcast_parser.add_argument('--bootstrap-port', type=int, default=18888, help='Bootstrap port for joining the network')
broadcast_parser.set_defaults(func='broadcast_discover')

def broadcast_discover(args):
    state = State(args.ip, port=args.port, interface=args.interface)

    dis = DiscoveryBroadcast(state, bootstrap_port=args.bootstrap_port, injected_sync=None)

    try:
        sync_info = dis.startJoin(sync_port=args.sync_port) # this arg is used for MQ i need to pass it here
    except Exception as e:
        print(f"Error during JOIN: {e}")
        exit(1)

    state.write_config()
    state.load_config()

    if sync_info["sync-type"] == "DHT":
        sync = SyncDHT(state, seed_node=(sync_info["sync-ip"], sync_info["sync-port"]), port=args.sync_port)
    elif sync_info["sync-type"] == "Gossip":
        sync = SyncGossip(state, seed_node=sync_info["sync-seed"], port=args.sync_port)
    elif sync_info["sync-type"] == "MQ":
        sync = MessageQueueSync(state, seed_node=sync_info["sync-seed"], port=args.sync_port)
    elif sync_info["sync-type"] == "ALL":
        print("Joining network with all synchronization methods enabled...")
        dht_info, gossip_info, mq_info = AllSync.splitInfo(sync_info)
        sync_dht = SyncDHT(state, seed_node=(dht_info["sync-ip"], dht_info["sync-port"]), port=args.sync_port-1)
        sync_gossip = SyncGossip(state, seed_node=gossip_info["sync-seed"], port=args.sync_port+1)
        sync_mq = MessageQueueSync(state, seed_node=mq_info["sync-seed"], port=args.sync_port, interval=args.change_check_interval)
        sync = AllSync(state, [sync_dht, sync_gossip, sync_mq])
        
    return state, sync, args.run

dnssd_parser = subparsers.add_parser('dnssd', help='join using DNSSD discovery')
add_common_args(dnssd_parser)
dnssd_parser.set_defaults(func='dnssd_discover')

def dnssd_discover(args):
    # state = State(args.ip, port=args.port, interface=args.interface)

    dis = DiscoveryDNSSD()

    info = dis.browseServices()
    if info:
        if info["type"] == "JOIN":
            args.bootstrap_port = info["port"]
            args.target_host = info["ip"]
            state, sync, run_flag = join_direct(args)
        elif info["type"] == "BROADCAST":
            args.bootstrap_port = info["port"]
            state, sync, run_flag = broadcast_discover(args)
        else:
            print("Unknown service type discovered via DNSSD.")
            exit(1)

    return state, sync, run_flag
    


create_parser = subparsers.add_parser('create', help='Create a new network')
add_common_args(create_parser)
create_parser.add_argument('--sync', required=True, type=str, help='Synchronization technology (DHT, Gossip, MQ, ALL)')
create_parser.set_defaults(func='create')

def create(args):
    state = State(args.ip, port=args.port, interface=args.interface)
    state.write_config()
    state.load_config()
    
    sync = None
    if args.sync == "DHT":
        sync = SyncDHT(state, port=args.sync_port, interval=args.change_check_interval)
    elif args.sync == "Gossip":
        sync = SyncGossip(state, port=args.sync_port)
    elif args.sync == "MQ":
        sync = MessageQueueSync(state, seed_node=None, port=args.sync_port, interval=args.change_check_interval)
    elif args.sync == "ALL":
        print("Creating network with all synchronization methods enabled...")
        sync_dht = SyncDHT(state, port=args.sync_port, interval=args.change_check_interval)
        sync_gossip = SyncGossip(state, port=args.sync_port+1)
        sync_mq = MessageQueueSync(state, seed_node=None, port=args.sync_port+2, interval=args.change_check_interval)
        sync = AllSync(state, [sync_dht, sync_gossip, sync_mq])
    else:
        print("Unsupported synchronization method specified.")
        exit(1)
    
    print("Network created successfully.")
    print(state.get_config())
    
    
    return state, sync, args.run


def main():
    args = parser.parse_args()
    
    if args.func == 'join-direct':
        state, sync, run_flag = join_direct(args)
    elif args.func == 'create':
        state, sync, run_flag = create(args)
    elif args.func == 'broadcast_discover':
        state, sync, run_flag = broadcast_discover(args)
    elif args.func == 'dnssd_discover':
        state, sync, run_flag = dnssd_discover(args)
    
    disc_join = None
    disc_bcast = None
    ad = None

    help_msg = """Available commands:
- exit : Exit the program
- return : Return to shell without stopping the network
- discover-join : Start accepting discovery join requests
- discover-broadcast : Start listening for broadcast requests
- advertise : Advertise direct joinability using DNSSD
- help : Show this help message
- info : Show current state information
"""
    print(help_msg)
    while run_flag:
        input_val = input()
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
            state.disable_config()
            state.disableNetlink()
            if ad:
                ad.stopAdvertise()
            break

        if input_val == "return": # return to shell
            # stop and leave config intact
            break
        elif input_val == "help":
            print(help_msg)

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
                if disc_bcast is not None or (disc_bcast is not None and disc_bcast.running):
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
    
if __name__ == "__main__":
    main()
