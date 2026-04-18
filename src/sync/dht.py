
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
        """ 
        Constructor for SyncDHT.
        Initializes the DHT server, starts the listening loop, and performs initial synchronization if a seed node is provided. 
        
        """
        
        self._state = injected_state
        self._port = port
        self.interval = interval
        """ Interval in seconds to check for changes in the DHT. """
        self._CurrentChangeCheckValue = -1

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
            asyncio.run_coroutine_threadsafe(self._listener_dht.listen(port, self._state.ip), self._listener_loop).result()
            asyncio.run_coroutine_threadsafe(self._listener_dht.bootstrap([seed_node]), self._listener_loop).result()
            asyncio.run_coroutine_threadsafe(self._dht.listen(port-1, self._state.ip), self._server_loop).result()
            asyncio.run_coroutine_threadsafe(self._dht.bootstrap([seed_node, (self._state.ip, port)]), self._server_loop).result()
            
            print(f"[DHT] DHT server started on port:{self._port} with bootstrap node {seed_node}")

            # to trigger replication
            self._setValueSync(self._state.ip, {
                "virtual_ip": self._state.ip,
                "public_key": self._state.public_key,
                "endpoint_ip": self._state.public_ip,
                "endpoint_port": self._state.public_port
            })


        else:
            asyncio.run_coroutine_threadsafe(self._listener_dht.listen(port, self._state.ip), self._listener_loop).result()
            
            asyncio.run_coroutine_threadsafe(self._dht.listen(port-1, self._state.ip), self._server_loop).result()
            asyncio.run_coroutine_threadsafe(self._dht.bootstrap([(self._state.ip, port)]), self._server_loop).result()

            self._setValueSync(CHANGE_CHECK_KEY, 0)
            self._setValueSync(KEY_LIST_KEY, [self._state.ip])
            self._setValueSync(self._state.ip, {
                "virtual_ip": self._state.ip,
                "public_key": self._state.public_key,
                "endpoint_ip": self._state.public_ip,
                "endpoint_port": self._state.public_port
            })
            print(f"[DHT] DHT server started on port:{self._port}")

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
        return ret.result()


    def _getValueSync(self, key: str) -> dict | int | None:
        """ Helper function to get a value from the DHT synchronously. """
        ret = asyncio.run_coroutine_threadsafe(self._dht.get(key), self._server_loop).result()
        if not ret:
            return None
        ret = json.loads(ret)

        return ret

    def getInfo(self)-> dict:
        """ Returns information about the synchronization module. Used for discovery purposes. """
        info = {
            "sync-type": "DHT",
            "sync-ip": self._state.ip,
            "sync-port": self._port
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
        self._CurrentChangeCheckValue = self._CurrentChangeCheckValue + 1

        key_list = self._getValueSync(KEY_LIST_KEY)
        key_list.append(virtual_ip)
        self._setValueSync(KEY_LIST_KEY, key_list)

    def _checkForChangeTrigger(self) -> None:
        """ 
        Checks if there have been changes in the DHT by comparing the current change check value with the local one. 
        If there are changes, it triggers checkForChanges. 
        """
        current_value = self._getValueSync(CHANGE_CHECK_KEY)
        if current_value != self._CurrentChangeCheckValue:
            if self._CurrentChangeCheckValue != -1:
                print(f"[DHT] Detected changes in DHT... value changed from {self._CurrentChangeCheckValue} to {current_value}")
            if self._CurrentChangeCheckValue > current_value:
                self._CurrentChangeCheckValue = -1
                print(f"[DHT] trying to resync with DHT")
                self._checkForChangeTrigger()
                return
            self.checkForChanges()
            self._CurrentChangeCheckValue = current_value


    def checkForChanges(self) -> None:
        """ 
        Checks for changes in the DHT by fetching the list of keys from the DHT and comparing the peer information for each key with the local state. 
        If there are changes, it updates the local state accordingly. 
        """
        self._state.lock_aquire(self)

        key_list = self._getValueSync(KEY_LIST_KEY)
        self._setValueSync(KEY_LIST_KEY, key_list)  # re-store to trigger replication

        for key in key_list:
            
            try:
                peer_info = self._getValueSync(key)
            except KeyError:
                # try next update cycle not synced yet
                continue
            
            try:
                existing_peer = self._state.peers[peer_info['virtual_ip']]
            except KeyError:

                if peer_info["virtual_ip"] == self._state.ip:
                    continue

                self._state.add_peer(
                    peer_info["virtual_ip"],
                    peer_info["public_key"],
                    peer_info["endpoint_ip"],
                    peer_info["endpoint_port"]
                )
                print(f"[DHT] Added new peer: {peer_info['virtual_ip']} -> {peer_info}")
                continue
            
            except TypeError:
                # Peer has been removed from DHT
                if key in self._state.peers.keys():
                    self._state.remove_peer(key)
                    print(f"[DHT] Removed peer from DHT: {key}")
                    continue

            if peer_info == None:
                continue
            self.check_individual_peer_change(peer_info, existing_peer)
        
        self._state.lock_release()


    def exitSync(self) -> None:
        """ Exits and cleans up the synchronization module. """
        print("[DHT] Exiting DHT synchronization...")
        if self.termination_event:
            self._loop.call_soon_threadsafe(self.termination_event.set)

        self._setValueSync(self._state.ip, None)

        old = self._getValueSync(CHANGE_CHECK_KEY)
        if old is None:
            old = -2
        self._setValueSync(CHANGE_CHECK_KEY, old + 1)

        self._dht.stop()
        self._listener_dht.stop()
