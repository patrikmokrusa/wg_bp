
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
""" Key that holds the current version of the DHT. """
KEY_LIST_KEY = "KEY_LIST"
""" Key that holds the list of keys that represent peers in the DHT. """

class SyncDHT(SyncBase):
    """ DHT synchronization module using Kademlia. Each peer runs a DHT server and stores its state in the DHT under its virtual IP as the key. """
    def __init__(self, injected_state: State, seed_node: tuple | None = None, port: int = 6881, interval: int = 5) -> None:
        """ Constructor for SyncDHT. Initializes the DHT server, starts the listening loop, and performs initial synchronization if a seed node is provided. """
        
        self.state = injected_state
        self.port = port
        self.interval = interval
        """ Interval in seconds to check for changes in the DHT. """
        self.CurrentChangeCheckValue = -1
        """ The value of local DHT state version. """

        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(target=self._run_loop, args=(self._loop,), daemon=True)
        self._loop_thread.start()
        
        self._listener_loop = asyncio.new_event_loop()
        self._listener_dht = Server()
        self._listener_thread = threading.Thread(target=self._run_loop, args=(self._listener_loop,), daemon=True)
        self._listener_thread.start()
        self._server_loop = asyncio.new_event_loop()
        self._dht = Server()
        self._server_thread = threading.Thread(target=self._run_loop, args=(self._server_loop,), daemon=True)
        self._server_thread.start()
        
        if seed_node:
            asyncio.run_coroutine_threadsafe(self._listener_dht.listen(port, self.state.ip), self._listener_loop).result()
            asyncio.run_coroutine_threadsafe(self._listener_dht.bootstrap([seed_node]), self._listener_loop).result()
            asyncio.run_coroutine_threadsafe(self._dht.listen(port-1, self.state.ip), self._server_loop).result()
            asyncio.run_coroutine_threadsafe(self._dht.bootstrap([seed_node, (self.state.ip, port)]), self._server_loop).result()
            
            print(f"[DHT] DHT server started on port:{self.port} with bootstrap node {seed_node}")

            # to trigger replication
            self._setValueSync(self.state.ip, {
                "virtual_ip": self.state.ip,
                "public_key": self.state.public_key,
                "endpoint_ip": self.state.public_ip,
                "endpoint_port": self.state.public_port
            })


        else:
            asyncio.run_coroutine_threadsafe(self._listener_dht.listen(port, self.state.ip), self._listener_loop).result()
            
            asyncio.run_coroutine_threadsafe(self._dht.listen(port-1, self.state.ip), self._server_loop).result()
            asyncio.run_coroutine_threadsafe(self._dht.bootstrap([(self.state.ip, port)]), self._server_loop).result()

            self._setValueSync(CHANGE_CHECK_KEY, 0)
            self._setValueSync(KEY_LIST_KEY, [self.state.ip])
            self._setValueSync(self.state.ip, {
                "virtual_ip": self.state.ip,
                "public_key": self.state.public_key,
                "endpoint_ip": self.state.public_ip,
                "endpoint_port": self.state.public_port
            })
            print(f"[DHT] DHT server started on port:{self.port}")

        asyncio.run_coroutine_threadsafe(self._async_init(), self._loop)

    def _run_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """ Helper function to run an asyncio event loop. """
        asyncio.set_event_loop(loop)
        loop.run_forever()
    
    async def _async_init(self)-> None:
        """ Async initialization function to set up the periodic check for changes in the DHT. """
        self.termination_event = asyncio.Event()
        self.task = asyncio.create_task(self._check_for_changes_loop())

    async def _check_for_changes_loop(self):
        """ Periodic loop to check for changes in the DHT. """
        while True:
            self._checkForChangeTrigger()
            try:
                await asyncio.wait_for(self.termination_event.wait(), timeout=self.interval)
                break 
            except asyncio.TimeoutError:
                pass

    def _setValueSync(self, key: str, value: dict | int | None) -> None:
        """ Helper function to set a value in the DHT synchronously. """
        value = json.dumps(value)  # Convert value to JSON string for storage


        ret = asyncio.run_coroutine_threadsafe(self._dht.set(key, value), self._server_loop)
        # print(f"[DHT] Set key '{key}' to value '{value}' in DHT")
        return ret.result()


    def _getValueSync(self, key: str) -> dict | int | None:
        """ Helper function to get a value from the DHT synchronously. """
        ret = asyncio.run_coroutine_threadsafe(self._dht.get(key), self._server_loop).result()
        # print(f"[DHT] Got value for key '{key}' from DHT")
        if not ret:
            return None
        ret = json.loads(ret)

        return ret

    def getInfo(self)-> dict:
        """ Returns information about the synchronization module. Used for discovery purposes. """
        info = {
            "sync-type": "DHT",
            "sync-ip": self.state.ip,
            "sync-port": self.port
        }
        return info

    def publishChange(self, virtual_ip: str, public_key: str, endpoint_ip: str, endpoint_port: int, sync_port: int | None = None) -> None:
        """ Publishes a change to the DHT. """
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

    def _checkForChangeTrigger(self) -> None:
        """ 
        Checks if there have been changes in the DHT by comparing the current change check value with the local one. 
        If there are changes, it triggers checkForChanges. 
        """
        # print("[DHT] Checking for changes in DHT...")
        current_value = self._getValueSync(CHANGE_CHECK_KEY)
        if current_value != self.CurrentChangeCheckValue:
            if self.CurrentChangeCheckValue != -1:
                print(f"[DHT] Detected changes in DHT... value changed from {self.CurrentChangeCheckValue} to {current_value}")
            if self.CurrentChangeCheckValue > current_value:
                self.CurrentChangeCheckValue = -1
                print(f"[DHT] trying to resync with DHT")
                self._checkForChangeTrigger()
                return
            self.checkForChanges()
            self.CurrentChangeCheckValue = current_value


    # def _appendToKeyList(self, virtual_ip: str) -> None:
    #     """ Helper function to append a virtual IP to the key list in the DHT. This is used when a new peer is added to the DHT. """
    #     key_list = self._getValueSync(KEY_LIST_KEY)
    #     if virtual_ip not in key_list:
    #         key_list.append(virtual_ip)
    #         self._setValueSync(KEY_LIST_KEY, key_list)
        
    # def isInKeyList(self, virtual_ip: str) -> bool:
    #     """ Helper function to check if a virtual IP is in the key list in the DHT. This is used to check if a peer is already known in the DHT. """
    #     key_list = self._getValueSync(KEY_LIST_KEY)
    #     return virtual_ip in key_list
    
    # def removeFromKeyList(self, virtual_ip: str) -> None:
    #     """ Helper function to remove a virtual IP from the key list in the DHT. This is used when a peer is removed from the DHT. """
    #     key_list = self._getValueSync(KEY_LIST_KEY)
    #     if virtual_ip in key_list:
    #         key_list.remove(virtual_ip)
    #         self._setValueSync(KEY_LIST_KEY, key_list)

    # def checkIfExistsByKeyList(self) -> list:
    #     """ Helper function to check if a virtual IP exists in the key list in the DHT. This is used to check if a peer is already known in the DHT. """
    #     key_list = self._getValueSync(KEY_LIST_KEY)
    #     active_peers = []
    #     for key in key_list:
    #         if self._getValueSync(key) is not None:
    #             active_peers.append(key)
    #     return active_peers

    def checkForChanges(self) -> None:
        """ 
        Checks for changes in the DHT by fetching the list of keys from the DHT and comparing the peer information for each key with the local state. 
        If there are changes, it updates the local state accordingly. 
        """
        self.state.lock_aquire(self)
        # print(f"[DHT] Checking for changes in DHT... START")

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
                continue
            
            except TypeError:
                # Peer has been removed from DHT
                if key in self.state.peers.keys():
                    self.state.remove_peer(key)
                    print(f"[DHT] Removed peer from DHT: {key}")
                    continue

            if peer_info == None:
                continue
            self.check_individual_peer_change(peer_info, existing_peer)
        
        # print(f"[DHT] Finished checking for changes in DHT... END")
        self.state.lock_release()


    def exitSync(self) -> None:
        """ Exits and cleans up the synchronization module. """
        print("[DHT] Exiting DHT synchronization...")
        if self.termination_event:
            self._loop.call_soon_threadsafe(self.termination_event.set)

        self._setValueSync(self.state.ip, None)

        # print(f"[DHT] Removed own peer info from DHT: {self.state.ip} ")
        old = self._getValueSync(CHANGE_CHECK_KEY)
        if old is None:
            old = -2
        # print(f"[DHT] Incrementing change check value to trigger updates... old value: {old}")
        self._setValueSync(CHANGE_CHECK_KEY, old + 1)
        # print(f"[DHT] Incremented change check value to trigger updates")

        self._dht.stop()
        self._listener_dht.stop()
