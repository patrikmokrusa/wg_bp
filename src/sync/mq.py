# Autor: Patrik Mokruša (xmokrup00)
import json
import threading
import time
import zmq
import asyncio
from .base import SyncBase

from state import State

STATE_UPDATE = "STATE_UPDATE"
""" Represents state update message type. """
DEPARTURE_NOTICE = "DEPARTURE_NOTICE"
""" Represents departure notice message type. """
ONBOARD_NOTICE = "ONBOARD_NOTICE"
""" Represents onboard notice message type. """

class MessageQueueSync(SyncBase):
    """
    Message Queue synchronization module using ZeroMQ PUB/SUB pattern. 
    Each peer has a PUB socket to broadcast its state and a SUB socket to listen for updates from other peers.
    """
    def __init__(self, state: State, seed_node: dict | None = None, port: int = 5555, interval: float = 0.1) -> None:
        """ Constructor for MessageQueueSync. Initializes the PUB and SUB sockets, starts the listening loop, and performs initial synchronization if a seed node is provided. """
        self._state = state
        self._port = port
        self.interval = interval
        """ Interval for checking for messages in the listening loop. """
        self._peers = {}  # virtual_ip -> peer info
        self._version = 0

        self._context = zmq.Context()
        self._pub = self._context.socket(zmq.PUB)
        self._pub.bind(f"tcp://{self._state.ip}:{self._port}")
        print(f"[MQ] PUB bound to tcp://{self._state.ip}:{self._port}")
        self._sub_context = zmq.Context()
        self._sub = self._context.socket(zmq.SUB)
        self._sub.setsockopt_string(zmq.SUBSCRIBE, "")

        # add self to peers list
        self._peers[self._state.ip] = {
            "virtual_ip": self._state.ip,
            "public_key": self._state.public_key,
            "endpoint_ip": self._state.public_ip,
            "endpoint_port": self._state.public_port,
            "sync_port": self._port,
            "allowed_ips": self._state.allowed_ips
        }

        if seed_node:
            self._peers[seed_node['virtual_ip']] = seed_node
            self._sub.connect(f"tcp://{seed_node['virtual_ip']}:{seed_node['sync_port']}")
            print(f"[MQ] SUB connecting to tcp://{seed_node['virtual_ip']}:{seed_node['sync_port']}")

        self.ready_event = asyncio.Event()
        """ Event to signal that initial synchronization is complete when joining an existing network. """
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        self._loop_thread.start()
        self._createTask(self._async_init(seed_node=seed_node))

        if seed_node:
            self._createTask(self.ready_event.wait()).result()

    async def _async_init(self, seed_node: dict | None = None) -> None:
        """ Asynchronous initialization method to start the listening loop and perform initial synchronization if a seed node is provided. """
        self.terminate_event = asyncio.Event()
        self.listen_task = asyncio.create_task(self._listenForUpdates())

        if seed_node:
            print("[MQ] Waiting for initial state synchronization from seed node...")
            while self._version == 0:
                self._publishOnboard()
                # block until we recieve response
                try:
                    await asyncio.wait_for(self.ready_event.wait(), timeout=5)
                    break
                except asyncio.TimeoutError:
                    pass

    async def _listenForUpdates(self) -> None:
        """ Loop to listen for updates from other peers and handle them accordingly. """
        while not self.terminate_event.is_set():
            try:
                msg = self._sub.recv_string(flags=zmq.NOBLOCK)
                data = json.loads(msg)
                if data["from"] == f"{self._state.ip}:{self._port}":
                    print("[MQ] Ignoring message from self")
                    continue


                if data["type"] == STATE_UPDATE:
                    if data["version"] > self._version:
                        print(f"[MQ] Received {data['type']} from peer {data['from']}.")
                        if self._version == 0:
                            print("[MQ] Initial state synchronization complete.")
                            self.ready_event.set()

                        self._version = data["version"]
                        self._peers = data["state"]
                        self.checkForChanges()

                    elif data["version"] < self._version:
                        print(f"[MQ] Received outdated {data['type']} from peer {data['from']}. Sending them our state")
                        self._publishState()
                    else:
                        print(f"[MQ] Received {data['type']} with same version from peer {data['from']}.")
                        continue
                
                elif data["type"] == DEPARTURE_NOTICE:
                    print(f"[MQ] Received {data['type']} from peer {data['from']}.")
                    del self._peers[data["virtual_ip"]]
                    self._sub.disconnect(f"tcp://{data['from']}")
                    self._version += 1
                    self._selfFix() # so i dont add back deleted peers by other modules in ALL sync
                    self.checkForChanges()

                elif data["type"] == ONBOARD_NOTICE:
                    print(f"[MQ] Received {data['type']} from peer {data['from']}.")
                    self._publishState()

                
            except zmq.Again:
                await asyncio.sleep(self.interval)

    def _selfFix(self) -> None:
        """ Fixes the MQ state by removing any peers that are not in the state peers list. This is to prevent adding back deleted peers by other modules in ALL sync. """
        for peer_ip in self._peers.keys():
            if peer_ip == self._state.ip:
                continue
            if peer_ip not in self._state.peers.keys():
                print(f"[MQ] Self-fix: Removing peer {peer_ip} which is not in state peers")
                self._sub.disconnect(f"tcp://{self._peers[peer_ip]['virtual_ip']}:{self._peers[peer_ip]['sync_port']}")
                del self._peers[peer_ip]

    def publishChange(self, virtual_ip: str, public_key: str, endpoint_ip: str, endpoint_port: int, 
                      allowed_ips: list | None = None, sync_port: int | None = None) -> None:
        """ Publishes a change to the MQ state."""
        val = {
            "virtual_ip": virtual_ip,
            "public_key": public_key,
            "endpoint_ip": endpoint_ip,
            "endpoint_port": endpoint_port
        }
        if allowed_ips:
            val["allowed_ips"] = allowed_ips
        else:
            val["allowed_ips"] = [virtual_ip + "/32"]
        self._peers[virtual_ip] = val
        if sync_port:
            self._peers[virtual_ip]["sync_port"] = sync_port
            self._sub.connect(f"tcp://{virtual_ip}:{sync_port}")
            print(f"[MQ] SUB connecting to tcp://{virtual_ip}:{sync_port}")

        if virtual_ip == self._state.ip: # to not break bootstrap when updating allowed ips
            self._peers[virtual_ip]["sync_port"] = self._port
        
        print(f"[MQ] Publishing changes to MQ...")
        self._version += 1
        self._publishState()

    def _publishOnboard(self) -> None:
        """ Sends and onboard notice to other peers. """
        msg = {
            "type": ONBOARD_NOTICE,
            "from": f"{self._state.ip}:{self._port}",
        }
        self._pub.send_string(json.dumps(msg))

    def _publishState(self) -> None:
        """ Sends the current state to other peers. """
        msg = {
            "type": STATE_UPDATE,
            "from": f"{self._state.ip}:{self._port}",
            "version": self._version,
            "state": self._peers
        }
        
        self._pub.send_string(json.dumps(msg))

    def _createTask(self, awaitable):
        """ Helper method to create a task in the asyncio loop from a different thread. """
        return asyncio.run_coroutine_threadsafe(awaitable, self._loop)
    
    def _run_loop(self):
        """ Helper method to set the event loop. """
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def getInfo(self) -> dict:
        """ Returns information about the synchronization method. Used for discovery purposes. """
        info = {
            "sync-type": "MQ",
            "sync-seed": self._peers[self._state.ip],
        }
        return info

    def checkForChanges(self) -> None:
        """ Checks for changes in the peers list and updates the state accordingly. This is called after receiving an update from another peer. """

        self._state.lock_aquire(self)

        for peer_ip, peer_info in self._peers.items():
            if peer_ip == self._state.ip:
                continue
            existing_peer = self._state.peers.get(peer_ip)
            if not existing_peer:
                self._state.add_peer(
                    peer_info["virtual_ip"],
                    peer_info["public_key"],
                    peer_info["endpoint_ip"],
                    peer_info["endpoint_port"]
                )
                self._sub.connect(f"tcp://{peer_info['virtual_ip']}:{peer_info['sync_port']}")
                print(f"[MQ] Added new peer: {peer_ip}")
                self._checkAllowedIPs(peer_info, self._state.peers.get(peer_ip))

            else:
                self._checkAllowedIPs(peer_info, existing_peer)

        existing_peers_copy = list(self._state.peers.items())
        for existing_peer_ip, existing_peer_info in existing_peers_copy:
            if existing_peer_ip not in self._peers.keys():
                self._state.remove_peer(existing_peer_ip)
                print(f"[MQ] Removed peer: {existing_peer_ip}")

        self._state.lock_release()

    def _checkAllowedIPs(self, peer_info: dict, existing_peer: dict) -> None:
        """ Helper method to check if allowed IPs have changed and update them if necessary. """
        if peer_info["allowed_ips"] != existing_peer["allowed_ips"]:
            self._state.set_peer_AllowedIPs(peer_info["virtual_ip"], peer_info["allowed_ips"])
            print(f"[MQ] Updated allowed IPs for {peer_info['virtual_ip']}.")

    def _publishLastMessage(self) -> None:
        """ Publishes a departure notice to other peers before shutting down. """
        msg = {
            "type": DEPARTURE_NOTICE,
            "from": f"{self._state.ip}:{self._port}",
            "virtual_ip": self._state.ip
        }
        print(f"[MQ] Publishing departure notice")
        self._pub.send_string(json.dumps(msg))
        time.sleep(2) # give some time for message to be sent before shutting down sockets
        


    def exitSync(self) -> None:
        """ Exits and cleans up the synchronization module. """
        print(f"[MQ] Shutting down Message Queue synchronization...")
        self._publishLastMessage()
        self.terminate_event.set()
        self.listen_task.cancel()
        self._sub.close()
        self._pub.close()
        self._context.term()