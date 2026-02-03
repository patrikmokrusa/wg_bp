
import time
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
            # Initialize own peer info and keys when joining for redundancy
            self.dht[self.state.ip] = {
                "virtual_ip": self.state.ip,
                "public_key": self.state.public_key,
                "endpoint_ip": self.state.public_ip,
                "endpoint_port": self.state.public_port
            }
        else:
            self.dht = DHT(self.state.ip, port)
            self.dht[CHANGE_CHECK_KEY] = 0
            self.dht[KEY_LIST_KEY] = [self.state.ip]
            self.dht[self.state.ip] = {
                "virtual_ip": self.state.ip,
                "public_key": self.state.public_key,
                "endpoint_ip": self.state.public_ip,
                "endpoint_port": self.state.public_port
            }

    def initSync(self):
        print("Initializing DHT synchronization...")

    def getInfo(self):
        info = {
            "sync-type": "DHT",
            "sync-ip": self.state.ip,
            "sync-port": self.port
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
        self.CurrentChangeCheckValue = self.CurrentChangeCheckValue + 1

    def checkForChanges(self):
        current_value = self.dht[CHANGE_CHECK_KEY]
        # print(f"TEST:   val from {self.CurrentChangeCheckValue} to {current_value}")
        if current_value != self.CurrentChangeCheckValue:
            if self.CurrentChangeCheckValue != -1:
                print(f"Detected changes in DHT... value changed from {self.CurrentChangeCheckValue} to {current_value}")
            self.fetchAndUpdateIter()
            self.CurrentChangeCheckValue = current_value
        
        
    def appendToKeyList(self, virtual_ip):

        key_list = self.dht[KEY_LIST_KEY]
        if virtual_ip not in key_list:
            key_list.append(virtual_ip)
            self.dht[KEY_LIST_KEY] = key_list
        
    def isInKeyList(self, virtual_ip):
        key_list = self.dht[KEY_LIST_KEY]
        return virtual_ip in key_list
    
    def fetchAndUpdateIter(self):
        change_happened = False

        key_list = self.dht[KEY_LIST_KEY]
        self.dht[KEY_LIST_KEY] = key_list  # re-store to trigger replication

        for key in key_list:
            
            try:
                peer_info = self.dht[key]
            except KeyError:
                # try next update cycle not synced yet
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
            
            except TypeError:
                # Peer has been removed from DHT
                self.state.remove_peer(key)
                print(f"Removed peer from DHT: {key} ")
                change_happened = True
                continue

            if self.check_individual_peer_change(peer_info, existing_peer):
                change_happened = True

        if change_happened:
            self.state.reload_config()

    def check_individual_peer_change(self, peer_info, existing_peer):
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
                print(f"""
                      Updated from DHT:
                      Before:
                      {peer_info['virtual_ip']} -> {existing_peer}
                      After:
                      {peer_info['virtual_ip']} -> {peer_info}
                      """)
                return True
        return False

    def exitSync(self):
        print("Exiting DHT synchronization...")
        
        self.dht[self.state.ip] = None
        
        self.dht[CHANGE_CHECK_KEY] = self.dht[CHANGE_CHECK_KEY] + 1

        time.sleep(1)  # give time to write the value
