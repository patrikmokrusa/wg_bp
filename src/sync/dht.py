
import threading
import time
from kad import DHT
from .base import SyncBase
import asyncio

CHANGE_CHECK_KEY = "CHANGE_CHECK"
KEY_LIST_KEY = "KEY_LIST"

class SyncDHT(SyncBase):
    def __init__(self, injected_state, seed_node=None, port=6881, interval=5):
        self.state = injected_state
        self.port = port
        self.interval = interval
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

        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        self.loop_thread.start()

        asyncio.run_coroutine_threadsafe(self._async_init(), self.loop)

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()
    
    async def _async_init(self):
        self.termination_event = asyncio.Event()
        self.task = asyncio.create_task(self._check_for_changes_loop())

    async def _check_for_changes_loop(self):
        while True:
            # print("[*] Checking for changes in DHT...")
            self.checkForChanges()
            try:
                await asyncio.wait_for(self.termination_event.wait(), timeout=self.interval)
                break 
            except asyncio.TimeoutError:
                pass

    def initSync(self):
        print("[DHT] Initializing DHT synchronization...")

    def getInfo(self):
        info = {
            "sync-type": "DHT",
            "sync-ip": self.state.ip,
            "sync-port": self.port
        }
        return info

    def publishChange(self, virtual_ip, public_key, endpoint_ip, endpoint_port, sync_port=None):
        print("[DHT] Publishing changes to DHT...")

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
        if current_value != self.CurrentChangeCheckValue:
            if self.CurrentChangeCheckValue != -1:
                print(f"[DHT] Detected changes in DHT... value changed from {self.CurrentChangeCheckValue} to {current_value}")
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
                print(f"[DHT] Added new peer: {peer_info['virtual_ip']} -> {peer_info}")
                change_happened = True
                continue
            
            except TypeError:
                # Peer has been removed from DHT
                self.state.remove_peer(key)
                print(f"[DHT] Removed peer from DHT: {key} ")
                change_happened = True
                continue

            if self.check_individual_peer_change(peer_info, existing_peer):
                change_happened = True

        if change_happened:
            self.state.reload_config()


    def exitSync(self):
        print("[DHT] Exiting DHT synchronization...")
        
        self.dht[self.state.ip] = None
        
        self.dht[CHANGE_CHECK_KEY] = self.dht[CHANGE_CHECK_KEY] + 1

        if self.termination_event:
            self.loop.call_soon_threadsafe(self.termination_event.set)

        

        # time.sleep(1)  # give time to write the value
