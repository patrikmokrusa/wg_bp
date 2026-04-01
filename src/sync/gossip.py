from ast import literal_eval
import asyncio
import threading
import json
import random

from state import State
from .base import SyncBase

STATE_UPDATE = "STATE_UPDATE"

class SyncGossip(SyncBase):
    def __init__(self, injected_state : State, seed_node: dict | None=None, port: int=6888, interval: int=5) -> None:
        print("Initializing Gossip synchronization...")
        self.interval = interval
        self.state = injected_state
        self.port = port
        self.seed_node = seed_node

        self.sendUpdates = True
        self.shutdown_event = None  # Will be created in async context
        self.gossip_task = None
        self.version = 0
        self.peers = {}
        self.peers[self.state.ip] = {
            "virtual_ip": self.state.ip,
            "public_key": self.state.public_key,
            "endpoint_ip": self.state.public_ip,
            "endpoint_port": self.state.public_port,
            "sync_port": self.port
        }

        if self.seed_node:
            print(f"[Gossip] Bootstrapping to seed node at {self.seed_node}...")
            self.peers[self.seed_node["virtual_ip"]] = self.seed_node

        # Create event loop in separate thread
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        self.loop_thread.start()
        
        # Start async initialization in the event loop
        self.createTask(self._async_init())
    
    async def _async_init(self) -> None:
        self.shutdown_event = asyncio.Event()
        self.server = await asyncio.start_server(self._handleGossip, self.state.ip, self.port)
        print(f"[Gossip] Gossip server listening on {self.state.ip}:{self.port}")
        
        # Start gossip heartbeat
        self.gossip_task = asyncio.create_task(self._sendGossip())


    async def _handleGossip(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        msg = json.loads((await reader.readline()).decode())
        # print(f"[*] Received Gossip message")
        from_ip, from_port = msg["from"].split(":")

        if msg["version"] == 0:
            # onboarding
            new_peer = msg["state"][from_ip]
            self.peers[new_peer["virtual_ip"]] = new_peer
            print(f"[Gossip] Onboarded new peer: {new_peer['virtual_ip']}")
            await self._sendStateToPeer(from_ip, from_port)
        elif msg["version"] < self.version:
            # update the other peer to our version and send them our state
            await self._sendStateToPeer(from_ip, from_port)
            return
        elif msg["version"] == self.version:
            return
        else:
            print(f"[Gossip] Received state update from peer {from_ip}:{from_port}.")
            self.version = msg["version"]
            self.peers = msg["state"]
            self.checkForChanges()

    async def _sendGossip(self) -> None:
        while True:
            known_peers = self.peers.keys()
            if len(known_peers) == 1:
                try:
                    await asyncio.wait_for(self.shutdown_event.wait(), timeout=self.interval)
                    break
                except asyncio.TimeoutError:
                    continue
            
            while True:
                random_peer_ip = random.choice(list(known_peers))
                if random_peer_ip == self.state.ip:
                    continue
                else:
                    break
            try:
                await self._sendStateToPeer(random_peer_ip, self.peers[random_peer_ip]["sync_port"])
            except Exception as e:
                print(f"[!] Error sending gossip to peer {random_peer_ip}: {e}")
            
            # Wait for interval or shutdown event
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=self.interval)
                break 
            except asyncio.TimeoutError:
                pass  # Timeout, continue gossip

    async def _sendStateToPeer(self, peer_ip: str, peer_port: int | None) -> None:
        if peer_port is None:
            print(f"Peer {peer_ip} not onboarded yet, cannot send state.")
            return

        reader, writer = await asyncio.open_connection(peer_ip, peer_port)
        msg = {
            "type": STATE_UPDATE,
            "version": self.version,
            "from": f"{self.state.ip}:{self.port}",
            "state": self.peers
        }
        writer.write((json.dumps(msg) + "\n").encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()


    def initSync(self) -> None:
        print(f"[Gossip] Initializing Gossip synchronization...")

    def getInfo(self) -> dict:
        info = {
            "sync-type": "Gossip",
            "sync-ip": self.state.ip,
            "sync-port": self.port,
            "sync-seed": self.peers[self.state.ip]
        }
        return info

    

    def publishChange(self, virtual_ip: str, public_key: str, endpoint_ip: str, endpoint_port: int, sync_port: int | None=None) -> None:
        print(f"[Gossip] Publishing changes to Gossip network...")
        self.version += 1
        msg = {
            "type": STATE_UPDATE,
            "version": self.version,
            "virtual_ip": virtual_ip,
            "public_key": public_key,
            "endpoint_ip": endpoint_ip,
            "endpoint_port": endpoint_port,
            "sync_port": self.port
        }
        self.peers[virtual_ip] = {
            "virtual_ip": virtual_ip,
            "public_key": public_key,
            "endpoint_ip": endpoint_ip,
            "endpoint_port": endpoint_port,
            "sync_port": None
        }

        if virtual_ip == self.state.ip:
            self.peers[virtual_ip]["sync_port"] = self.port

        print(f"[Gossip] Successfully published change: {msg}")


    async def _sendLastGossip(self) -> None:
        # Send ourselves still in state, but set flag so others know we're leaving
        self.version += 1
        del self.peers[self.state.ip]

        if len(self.peers) > 0:
            random_peer_ip = random.choice(list(self.peers.keys()))
        
            try:
                await self._sendStateToPeer(
                    random_peer_ip, 
                    self.peers[random_peer_ip]["sync_port"]
                )
                print(f"[Gossip] Departure message sent to {random_peer_ip}")
            except Exception as e:
                print(f"[!] Error sending departure: {e}")

    def exitSync(self) -> None:
        print(f"[Gossip] Exiting Gossip synchronization...")
        self.sendUpdates = False
        
        # Signal shutdown to wake up the gossip task
        if self.shutdown_event:
            self.loop.call_soon_threadsafe(self.shutdown_event.set)
        
        # Send final state
        try:
            self.createTask(self._sendLastGossip()).result(timeout=1)
        except:
            pass
        
        # Close server
        if self.server:
            self.server.close()
        
        # Wait for gossip task to finish
        if self.gossip_task:
            try:
                self.createTask(asyncio.sleep(1)).result(timeout=2)
                self.gossip_task.result()
            except:
                pass
        
        # Stop loop cleanly
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.loop_thread.join(timeout=2)
    
    def createTask(self, awaitable: asyncio.coroutine) -> asyncio.Future:
        """Create a task in the event loop"""
        return asyncio.run_coroutine_threadsafe(awaitable, self.loop)
    
    def _run_loop(self):
        """Run the event loop"""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def checkForChanges(self) -> None:
        self.state.lock_aquire()

        reload_required = False
        for peer_ip, peer_info in self.peers.items():
            existing_peer = self.state.peers.get(peer_ip)
            if not existing_peer:
                self.state.add_peer(
                    peer_info["virtual_ip"],
                    peer_info["public_key"],
                    peer_info["endpoint_ip"],
                    peer_info["endpoint_port"]
                )
                print(f"[Gossip] Added new peer: {peer_ip} -> {peer_info}\n")
                print(self.state.get_config())
                reload_required = True
            else:
                if self.check_individual_peer_change(peer_info, existing_peer):
                    reload_required = True

        # check for deleted peers - make a copy to avoid dict size change during iteration
        existing_peers_copy = list(self.state.peers.items())
        for existing_peer_ip, existing_peer_info in existing_peers_copy:
            if existing_peer_ip not in self.peers.keys():
                self.state.remove_peer(existing_peer_ip)
                print(f"[Gossip] Removed peer: {existing_peer_ip} -> {existing_peer_info}\n")
                print(self.state.get_config())
                reload_required = True
        if reload_required:
            self.state.reload_config()

        self.state.lock_release()
        
        