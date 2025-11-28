
from kad import DHT
from .base import SyncBase

CHANGE_CHECK_KEY = "CHANGE_CHECK"
KEY_LIST_KEY = "KEY_LIST"

class SyncDHT(SyncBase):
    def __init__(self, injected_state, seed_node=None, port=6881):
        self.state = injected_state
        self.port = port
        self.CurrentChangeCheckValue = -1
        
        if seed_node:
            self.dht = DHT(self.state.ip, port, seeds=seed_node)
        else:
            self.dht = DHT(self.state.ip, port)
            self.dht[CHANGE_CHECK_KEY] = 0
            self.dht[KEY_LIST_KEY] = [self.state.ip]
            self.dht[self.state.ip] = {
                "virtual_ip": self.state.ip,
                "public_key": self.state.public_key,
                "endpoint_ip": self.state.public_ip,
                "endpoint_port": self.state.port
            }

    def initSync(self):
        print("Initializing DHT synchronization...")

    def getInfo(self):
        info = {
            "sync-type": "DHT",
            "dht-ip": self.state.ip,
            "dht-port": self.port
        }
        return info
        

    def publishChange(self, virtual_ip, public_key, endpoint_ip, endpoint_port):
        print("Publishing changes to DHT...")

        self.dht[virtual_ip] = {
            "virtual_ip": virtual_ip,
            "public_key": public_key,
            "endpoint_ip": endpoint_ip,
            "endpoint_port": endpoint_port
        }

        if not self.isInKeyList(virtual_ip):
            self.appendToKeyList(virtual_ip)

        self.dht[CHANGE_CHECK_KEY] = self.dht[CHANGE_CHECK_KEY] + 1

    def checkForChanges(self):
        
        current_value = self.dht[CHANGE_CHECK_KEY]
        if current_value != self.CurrentChangeCheckValue:
            print("Detected changes in DHT...")
            self.CurrentChangeCheckValue = current_value
            self.fetchAndUpdateIter()
        
        
    def appendToKeyList(self, virtual_ip):

        key_list = self.dht[KEY_LIST_KEY]
        if virtual_ip not in key_list:
            key_list.append(virtual_ip)
            self.dht[KEY_LIST_KEY] = key_list
        
    def isInKeyList(self, virtual_ip):
        key_list = self.dht[KEY_LIST_KEY]
        return virtual_ip in key_list
    
    def fetchAndUpdateIter(self):
        print("Fetching and updating data from DHT...")
        change_happened = False

        key_list = self.dht[KEY_LIST_KEY]

        for key in key_list:
            
            try:
                peer_info = self.dht[key]
            except KeyError:
                # try next update cycle
                continue
            

            try:
                existing_peer = self.state.peers[peer_info['virtual_ip']]
            except KeyError:

                if peer_info["virtual_ip"] == self.state.ip:
                    continue

                self.state.add_peer(
                    peer_info["virtual_ip"],
                    peer_info["public_key"],
                    peer_info["endpoint_ip"],
                    peer_info["endpoint_port"]
                )
                print(f"Added new peer from DHT: {peer_info['virtual_ip']} -> {peer_info}")
                change_happened = True
                continue


            if (peer_info["public_key"] != existing_peer["public_key"] or
                peer_info["endpoint_ip"] != existing_peer["endpoint_ip"] or
                peer_info["endpoint_port"] != existing_peer["endpoint_port"]):
                self.state.remove_peer(peer_info["virtual_ip"])
                self.state.add_peer(
                    peer_info["virtual_ip"],
                    peer_info["public_key"],
                    peer_info["endpoint_ip"],
                    peer_info["endpoint_port"]
                )
                print(f"Updated peer from DHT: {peer_info['virtual_ip']} -> {peer_info}")
                change_happened = True
    
        

        if change_happened:
            self.state.reload_config()