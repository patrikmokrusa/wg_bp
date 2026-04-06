
import threading
import json
import logging
from kademlia.network import Server
from .base import SyncBase
import asyncio
from state import State

for logger_name in ("kademlia", "rpcudp"):
    lib_logger = logging.getLogger(logger_name)
    lib_logger.setLevel(logging.CRITICAL)
    lib_logger.propagate = False

CHANGE_CHECK_KEY = "CHANGE_CHECK"
KEY_LIST_KEY = "KEY_LIST"

class SyncDHT(SyncBase):
    def __init__(self, injected_state: State, seed_node: tuple | None = None, port: int = 6881, interval: int = 5) -> None:
        self.state = injected_state
        self.port = port
        self.interval = interval
        self.CurrentChangeCheckValue = -1

        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self._run_loop, args=(self.loop,), daemon=True)
        self.loop_thread.start()
        
        self.listener_loop = asyncio.new_event_loop()
        self.listener_dht = Server()
        self.listener_thread = threading.Thread(target=self._run_loop, args=(self.listener_loop,), daemon=True)
        self.listener_thread.start()
        self.server_loop = asyncio.new_event_loop()
        self.dht = Server()
        self.server_thread = threading.Thread(target=self._run_loop, args=(self.server_loop,), daemon=True)
        self.server_thread.start()
        
        if seed_node:
            asyncio.run_coroutine_threadsafe(self.listener_dht.listen(port, self.state.ip), self.listener_loop).result()
            asyncio.run_coroutine_threadsafe(self.listener_dht.bootstrap([seed_node]), self.listener_loop).result()
            asyncio.run_coroutine_threadsafe(self.dht.listen(port-1, self.state.ip), self.server_loop).result()
            asyncio.run_coroutine_threadsafe(self.dht.bootstrap([seed_node, (self.state.ip, port)]), self.server_loop).result()
            
            print(f"[DHT] DHT server started on port:{self.port} with bootstrap node {seed_node}")

            # to trigger replication
            self._setValueSync(self.state.ip, {
                "virtual_ip": self.state.ip,
                "public_key": self.state.public_key,
                "endpoint_ip": self.state.public_ip,
                "endpoint_port": self.state.public_port
            })


        else:
            asyncio.run_coroutine_threadsafe(self.listener_dht.listen(port, self.state.ip), self.listener_loop).result()
            
            asyncio.run_coroutine_threadsafe(self.dht.listen(port-1, self.state.ip), self.server_loop).result()
            asyncio.run_coroutine_threadsafe(self.dht.bootstrap([(self.state.ip, port)]), self.server_loop).result()

            self._setValueSync(CHANGE_CHECK_KEY, 0)
            self._setValueSync(KEY_LIST_KEY, [self.state.ip])
            self._setValueSync(self.state.ip, {
                "virtual_ip": self.state.ip,
                "public_key": self.state.public_key,
                "endpoint_ip": self.state.public_ip,
                "endpoint_port": self.state.public_port
            })
            print(f"[DHT] DHT server started on port:{self.port}")

        asyncio.run_coroutine_threadsafe(self._async_init(), self.loop)

    def _run_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()
    
    async def _async_init(self)-> None:
        self.termination_event = asyncio.Event()
        self.task = asyncio.create_task(self._check_for_changes_loop())

    async def _check_for_changes_loop(self):
        while True:
            self.checkForChangeTrigger()
            try:
                await asyncio.wait_for(self.termination_event.wait(), timeout=self.interval)
                break 
            except asyncio.TimeoutError:
                pass

    def _setValueSync(self, key: str, value: dict | int | None) -> None:
        value = json.dumps(value)  # Convert value to JSON string for storage


        ret = asyncio.run_coroutine_threadsafe(self.dht.set(key, value), self.server_loop)
        # print(f"[DHT] Set key '{key}' to value '{value}' in DHT")
        return ret.result()


    def _getValueSync(self, key: str) -> dict | int | None:
        ret = asyncio.run_coroutine_threadsafe(self.dht.get(key), self.server_loop).result()
        # print(f"[DHT] Got value for key '{key}' from DHT")
        if not ret:
            return None
        ret = json.loads(ret)

        return ret

    def initSync(self) -> None:
        print("[DHT] Initializing DHT synchronization...")

    def getInfo(self)-> dict:
        info = {
            "sync-type": "DHT",
            "sync-ip": self.state.ip,
            "sync-port": self.port
        }
        return info

    def publishChange(self, virtual_ip: str, public_key: str, endpoint_ip: str, endpoint_port: int, sync_port: int | None = None):
        print("[DHT] Publishing changes to DHT...")

        self._setValueSync(virtual_ip, {
            "virtual_ip": virtual_ip,
            "public_key": public_key,
            "endpoint_ip": endpoint_ip,
            "endpoint_port": endpoint_port
        })

        old = self._getValueSync(CHANGE_CHECK_KEY)
        self._setValueSync(CHANGE_CHECK_KEY, old + 1)
        self.CurrentChangeCheckValue = self.CurrentChangeCheckValue + 1

        key_list = self._getValueSync(KEY_LIST_KEY)
        key_list.append(virtual_ip)
        self._setValueSync(KEY_LIST_KEY, key_list)

    def checkForChangeTrigger(self) -> None:
        # print("[DHT] Checking for changes in DHT...")
        current_value = self._getValueSync(CHANGE_CHECK_KEY)
        if current_value != self.CurrentChangeCheckValue:
            if self.CurrentChangeCheckValue != -1:
                print(f"[DHT] Detected changes in DHT... value changed from {self.CurrentChangeCheckValue} to {current_value}")
            if self.CurrentChangeCheckValue > current_value:
                self.CurrentChangeCheckValue = -1
                print(f"[DHT] trying to resync with DHT")
                self.checkForChangeTrigger()
                return
            self.checkForCHanges()
            self.CurrentChangeCheckValue = current_value


    def appendToKeyList(self, virtual_ip: str) -> None:

        key_list = self._getValueSync(KEY_LIST_KEY)
        if virtual_ip not in key_list:
            key_list.append(virtual_ip)
            self._setValueSync(KEY_LIST_KEY, key_list)
        
    def isInKeyList(self, virtual_ip: str) -> bool:
        key_list = self._getValueSync(KEY_LIST_KEY)
        return virtual_ip in key_list
    
    def removeFromKeyList(self, virtual_ip: str) -> None:
        key_list = self._getValueSync(KEY_LIST_KEY)
        if virtual_ip in key_list:
            key_list.remove(virtual_ip)
            self._setValueSync(KEY_LIST_KEY, key_list)

    def checkIfExistsByKeyList(self) -> list:
        key_list = self._getValueSync(KEY_LIST_KEY)
        active_peers = []
        for key in key_list:
            if self._getValueSync(key) is not None:
                active_peers.append(key)
        return active_peers

    def checkForCHanges(self) -> None:
        self.state.lock_aquire(self)
        # print(f"[DHT] Checking for changes in DHT... START")
        change_happened = False

        key_list = self._getValueSync(KEY_LIST_KEY)
        # print(f"[DHT*****] Current key list from DHT: {key_list}")
        self._setValueSync(KEY_LIST_KEY, key_list)  # re-store to trigger replication

        for key in key_list:
            
            try:
                peer_info = self._getValueSync(key)
                # print(f"[DHT*****] Fetched peer info for key {key}: {peer_info}")
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
                # print(f"[DHT] tady se to zkurvilo")
                # print(f"[DHT] {key} self.state.peers.keys() {self.state.peers.keys()}")
                if key in self.state.peers.keys():
                    self.state.remove_peer(key)
                    print(f"[DHT] Removed peer from DHT: {key} ")
                    change_happened = True
                    continue

            if peer_info == None:
                continue
            if self.check_individual_peer_change(peer_info, existing_peer):
                change_happened = True
        if change_happened:
            self.state.reload_config()
        
        # print(f"[DHT] Finished checking for changes in DHT... END")
        self.state.lock_release()


    def exitSync(self) -> None:
        print("[DHT] Exiting DHT synchronization...")
        if self.termination_event:
            self.loop.call_soon_threadsafe(self.termination_event.set)

        self._setValueSync(self.state.ip, None)

        # print(f"[DHT] Removed own peer info from DHT: {self.state.ip} ")
        old = self._getValueSync(CHANGE_CHECK_KEY)
        if old is None:
            old = -2
        # print(f"[DHT] Incrementing change check value to trigger updates... old value: {old}")
        self._setValueSync(CHANGE_CHECK_KEY, old + 1)
        # print(f"[DHT] Incremented change check value to trigger updates")

        self.dht.stop()
        self.listener_dht.stop()
