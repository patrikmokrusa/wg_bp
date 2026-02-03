import threading
from change_checker import ChangeChecker
from state import State
from discovery.join import DiscoveryJoin
from sync.dht import SyncDHT
import argparse

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
        sync_info = dis.startJoin(args.target_host)
    except Exception as e:
        print(f"Error during JOIN: {e}")
        exit(1)


    state.write_config()
    state.load_config()

    if sync_info["sync-type"] == "DHT":
        sync = SyncDHT(state, seed_node=[(sync_info["sync-ip"], sync_info["sync-port"])], port=args.sync_port)


    return state, sync, args.run


create_parser = subparsers.add_parser('create', help='Create a new network')
add_common_args(create_parser)
create_parser.add_argument('--sync', required=True, type=str, help='Synchronization technology (e.g., DHT)')
create_parser.set_defaults(func='create')

def create(args):
    state = State(args.ip, port=args.port, interface=args.interface)
    state.write_config()
    state.load_config()
    
    sync = None
    if args.sync == "DHT":
        sync = SyncDHT(state, port=args.sync_port)
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
    
    disc = None
    print(sync.getInfo())

    change_checker = ChangeChecker(state, sync, interval=args.change_check_interval)
    change_checker.beginWork()

    help_msg = """Available commands:
- exit : Exit the program
- return : Return to shell without stopping the network
- discover-join : Start accepting discovery join requests
- help : Show this help message
- info : Show current state information
- force-check : Force an immediate check for changes in the network
"""
    print(help_msg)
    while run_flag:
        input_val = input()
        if input_val == "exit": # exit or ctrl + c
            change_checker.running = False
            if disc is not None and (disc is not None and disc.running):
                disc.stopAccept()
            sync.exitSync()
            state.disable_config()
            break

        if input_val == "return": # return to shell
            # stop and leave config intact
            break
        elif input_val == "help":
            print(help_msg)
            
        elif input_val == "info":
            print(state.get_config())
            print(f"state.public_ip: {state.public_ip} state.public_port: {state.public_port}")
            print(f"state.peers: {state.peers}")
            print(sync.getInfo())
            
        elif input_val == "force-check":
            change_checker.forceCheck()
            
        elif input_val == "discover-join":
            if disc is not None or (disc is not None and disc.running):
                print("Stopping previous discovery join...")
                disc.stopAccept()
                disc = None
                continue

            port = input("Enter port for discovery join (default 17777): ")
            if port == "":
                port = 17777
            else:
                port = int(port)
            disc = DiscoveryJoin(state, sync, bootstrap_port=port)
            try:
                bootstrap_thread = threading.Thread(target=disc.startAccept)
                bootstrap_thread.start()
            except Exception as e:
                print(f"Error in discovery join: {e}")

    
    change_checker.running = False

    
if __name__ == "__main__":
    main()
