from ast import literal_eval
import asyncio
import threading
import json
import random

from state import State
from .base import SyncBase

DEGREE = 2
""" Degree of gossip - how many peers to send each update to. """
STATE_UPDATE = "STATE_UPDATE"
""" Represents state update message type. """
DEPARTURE_NOTICE = "DEPARTURE_NOTICE"
""" Represents departure notice message type. """

class SyncGossip(SyncBase):
    """
    Gossip synchronization module for peer state synchronization. 
    """
    def __init__(self, injected_state : State, seed_node: dict | None=None, port: int=6888, interval: int=5) -> None:
        """
        Constructor for sync gossip module. Initializes the gossip state and starts the gossip heartbeat and server for receiving gossip messages.
        """
        print("Initializing Gossip synchronization...")
        self.interval = interval
        """ The interval (in seconds) at which to send gossip messages. """
        self._state = injected_state
        self._port = port
        self._seed_node = seed_node

        self._send_lock = threading.Lock()

        self._sendUpdates = True
        self._shutdown_event = None  # Will be created in async context
        self._gossip_task = None
        self._version = 0
        self._peers = {}

        self._peers[self._state.ip] = {
            "virtual_ip": self._state.ip,
            "public_key": self._state.public_key,
            "endpoint_ip": self._state.public_ip,
            "endpoint_port": self._state.public_port,
            "sync_port": self._port
        }

        if self._seed_node:
            print(f"[Gossip] Bootstrapping to seed node at {self._seed_node}...")
            self._peers[self._seed_node["virtual_ip"]] = self._seed_node

        # Create event loop in separate thread
        self.loop = asyncio.new_event_loop()
        """ Event loop for asynchronous gossip operations. """
        self.loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        """ Thread to run the event loop. """
        self.loop_thread.start()
        
        # Start async initialization in the event loop
        self._createTask(self._async_init())
    
    async def _async_init(self) -> None:
        """
        Initializes the sending gossip "heartbeat" and a server for receiving those messages.
        """
        self.shutdown_event = asyncio.Event()
        self.server = await asyncio.start_server(self._handleGossip, self._state.ip, self._port)
        print(f"[Gossip] Gossip server listening on {self._state.ip}:{self._port}")
        
        # Start gossip heartbeat
        self._gossip_task = asyncio.create_task(self._sendGossip())


    async def _handleGossip(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """ Handles incoming gossip messages from other peers. """
        msg = json.loads((await reader.readline()).decode())
        # print(f"[*] Received Gossip message")
        from_ip, from_port = msg["from"].split(":")

        if msg["version"] == 0:
            # onboarding
            new_peer = msg["state"][from_ip]

            # update only port
            self._peers[new_peer["virtual_ip"]]["sync_port"] = new_peer["sync_port"]
            print(f"[Gossip] Onboarded new peer: {new_peer['virtual_ip']}")
            self._version += 1
            await self._sendStateToPeer(from_ip, from_port)
        elif msg["version"] < self._version:
            # update the other peer to our version and send them our state
            await self._sendStateToPeer(from_ip, from_port)
            return
        elif msg["version"] == self._version:
            return
        else:
            print(f"[Gossip] Received state update from peer {from_ip}:{from_port}.")
            self._version = msg["version"]
            self._peers = msg["state"]
            self.checkForChanges()

        if msg["type"] == DEPARTURE_NOTICE:
            print(f"[Gossip] Received departure notice from {from_ip}:{from_port}")
            if from_ip in self._peers.keys():
                del self._peers[from_ip]
                self.checkForChanges()


    async def _sendGossip(self) -> None:
        """ Based on interval, randomly selects a subset of peers to send the full state to them. """
        while True:
            known_peers = self._peers.keys()
            if len(known_peers) == 1:
                try:
                    await asyncio.wait_for(self.shutdown_event.wait(), timeout=self.interval)
                    break
                except asyncio.TimeoutError:
                    continue
            
            contacted_peers = []

            for _ in range(min(DEGREE, len(known_peers)-1)):
                while True:
                    random_peer_ip = random.choice(list(known_peers))
                    if random_peer_ip == self._state.ip:
                        continue
                    elif random_peer_ip in contacted_peers:
                        continue
                    else:
                        break

                contacted_peers.append(random_peer_ip)
                
                try:
                    await self._sendStateToPeer(random_peer_ip, self._peers[random_peer_ip]["sync_port"])
                except Exception as e:
                    if random_peer_ip not in self._state.peers.keys():
                        del self._peers[random_peer_ip]
            
            # Wait for interval or shutdown event
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=self.interval)
                break 
            except asyncio.TimeoutError:
                pass  # Timeout, continue gossip

    async def _sendStateToPeer(self, peer_ip: str, peer_port: int | None, departure: bool = False) -> None:
        """ Sends the full state to a specific peer. """
        self._send_lock.acquire()

        if peer_port is None:
            print(f"[Gossip] Peer {peer_ip} not onboarded yet, cannot send state.")
            self._send_lock.release()
            return

        # Add timeout to connection attempt (e.g., 5 seconds)
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(peer_ip, peer_port), timeout=1
            )
        except Exception as e:
            # print(f"[*!*] Error connecting to peer {peer_ip}:{peer_port} - {e}")
            self._send_lock.release()
            raise
        
        msg = {
            "type": STATE_UPDATE,
            "version": self._version,
            "from": f"{self._state.ip}:{self._port}",
            "state": self._peers
        }

        if departure:
            msg["type"] = DEPARTURE_NOTICE

        writer.write((json.dumps(msg) + "\n").encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        self._send_lock.release()


    def getInfo(self) -> dict:
        """ Returns information about the synchronization module for discovery purposes. """
        info = {
            "sync-type": "Gossip",
            "sync-ip": self._state.ip,
            "sync-port": self._port,
            "sync-seed": self._peers[self._state.ip]
        }
        return info

    

    def publishChange(self, virtual_ip: str, public_key: str, endpoint_ip: str, endpoint_port: int, sync_port: int | None=None) -> None:
        """ Publishes a change to the gossip state. """
        print(f"[Gossip] Publishing changes to Gossip network...")
        self._version += 1
        msg = {
            "type": STATE_UPDATE,
            "version": self._version,
            "virtual_ip": virtual_ip,
            "public_key": public_key,
            "endpoint_ip": endpoint_ip,
            "endpoint_port": endpoint_port,
            "sync_port": self._port
        }
        self._peers[virtual_ip] = {
            "virtual_ip": virtual_ip,
            "public_key": public_key,
            "endpoint_ip": endpoint_ip,
            "endpoint_port": endpoint_port,
            "sync_port": None
        }

        if virtual_ip == self._state.ip:
            self._peers[virtual_ip]["sync_port"] = self._port

        # print(f"[Gossip] Successfully published change: {self._peers} version: {self._version}")


    async def _sendLastGossip(self) -> None:
        """ Sends a departure notice. Used when exiting the synchronization module. """
        self._version += 1
        del self._peers[self._state.ip]

        contacted_peers = []
        

        if len(self._peers) <= 0:
            return

        for _ in range(min(DEGREE, len(self._peers))):
            while True:
                random_peer_ip = random.choice(list(self._peers.keys()))
                if random_peer_ip in contacted_peers:
                    continue
                else:
                    break

            contacted_peers.append(random_peer_ip)
            
            try:
                await self._sendStateToPeer(random_peer_ip, self._peers[random_peer_ip]["sync_port"], departure=True)
                print(f"[Gossip] Departure message sent to {random_peer_ip}")
            except Exception as e:
                if random_peer_ip not in self._state.peers.keys():
                    del self._peers[random_peer_ip]
                    return
    

    def exitSync(self) -> None:
        """ Exits and cleanups the module. """
        print(f"[Gossip] Exiting Gossip synchronization...")
        self._sendUpdates = False
        
        # Signal shutdown to wake up the gossip task
        if self.shutdown_event:
            self.loop.call_soon_threadsafe(self.shutdown_event.set)
        
        # Wait for gossip task to finish
        if self._gossip_task:
            try:
                self._gossip_task.result(timeout=5)
            except:
                pass
    
        # Send final state
        try:
            self._createTask(self._sendLastGossip()).result(timeout=5)
        except:
            pass
        
        # Close server
        if self.server:
            self.server.close()
        
        
        # Stop loop cleanly
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.loop_thread.join(timeout=2)
    
    def _createTask(self, awaitable):
        """ Helper method to create a task in the event loop from a different thread. """
        return asyncio.run_coroutine_threadsafe(awaitable, self.loop)
    
    def _run_loop(self):
        """ Helper to run event loop. """
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def checkForChanges(self) -> None:
        """ Compares gossip state to the actual state and applies any necessary changes. """
        self._state.lock_aquire(self)
        # print(f"[Gossip] Checking for changes in peers")
        for peer_ip, peer_info in self._peers.items():
            existing_peer = self._state.peers.get(peer_ip)
            if not existing_peer:
                if peer_info["virtual_ip"] == self._state.ip:
                    continue
                self._state.add_peer(
                    peer_info["virtual_ip"],
                    peer_info["public_key"],
                    peer_info["endpoint_ip"],
                    peer_info["endpoint_port"]
                )
                print(f"[Gossip] Added new peer: {peer_ip}")
            else:
                if self.check_individual_peer_change(peer_info, existing_peer):
                    reload_required = True

        # print(f"[Gossip] Checked for changes in peers HALFWAY")
        # check for deleted peers - make a copy to avoid dict size change during iteration
        existing_peers_copy = list(self._state.peers.items())
        for existing_peer_ip, existing_peer_info in existing_peers_copy:
            if existing_peer_ip not in self._peers.keys():
                self._state.remove_peer(existing_peer_ip)
                print(f"[Gossip] Removed peer: {existing_peer_ip}\n")
        # print(f"[Gossip] Finished checking for changes in peers RELEASE LOCK")
        self._state.lock_release()
        
        