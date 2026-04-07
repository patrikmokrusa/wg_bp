import json
import threading
import time
import zmq
import asyncio
from .base import SyncBase

from state import State

STATE_UPDATE = "STATE_UPDATE"
DEPARTURE_NOTICE = "DEPARTURE_NOTICE"
ONBOARD_NOTICE = "ONBOARD_NOTICE"


class MessageQueueSync(SyncBase):
    def __init__(self, state: State, seed_node: dict | None = None, port: int = 5555, interval: float = 0.1) -> None:
        self.state = state
        self.port = port
        self.interval = interval
        self.peers = {}  # virtual_ip -> peer info
        self.version = 0

        self.pub_context = zmq.Context()
        self.pub = self.pub_context.socket(zmq.PUB)
        self.pub.bind(f"tcp://{self.state.ip}:{self.port}")
        print(f"[MQ] PUB bound to tcp://{self.state.ip}:{self.port}")
        self.sub_context = zmq.Context()
        self.sub = self.sub_context.socket(zmq.SUB)
        self.sub.setsockopt_string(zmq.SUBSCRIBE, "")

        # add self to peers list
        self.peers[self.state.ip] = {
            "virtual_ip": self.state.ip,
            "public_key": self.state.public_key,
            "endpoint_ip": self.state.public_ip,
            "endpoint_port": self.state.public_port,
            "sync_port": self.port
        }

        if seed_node:
            self.peers[seed_node['virtual_ip']] = seed_node
            self.sub.connect(f"tcp://{seed_node['virtual_ip']}:{seed_node['sync_port']}")
            print(f"[MQ] SUB connecting to tcp://{seed_node['virtual_ip']}:{seed_node['sync_port']}")

        self.ready_event = asyncio.Event()
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        self.loop_thread.start()
        self.createTask(self._async_init(seed_node=seed_node))

        if seed_node:
            self.createTask(self.ready_event.wait()).result()

    async def _async_init(self, seed_node: dict | None = None) -> None:
        self.terminate_event = asyncio.Event()
        self.listen_task = asyncio.create_task(self._listenForUpdates())

        if seed_node:
            print("[MQ] Waiting for initial state synchronization from seed node...")
            while self.version == 0:
                self.publishOnboard()
                # block until we recieve response
                try:
                    await asyncio.wait_for(self.ready_event.wait(), timeout=5)
                    break
                except asyncio.TimeoutError:
                    pass

    async def _listenForUpdates(self) -> None:
        self_fix_cnt = 0
        while not self.terminate_event.is_set():
            try:
                msg = self.sub.recv_string(flags=zmq.NOBLOCK)
                data = json.loads(msg)
                if data["from"] == f"{self.state.ip}:{self.port}":
                    print("[MQ] Ignoring message from self")
                    continue


                if data["type"] == STATE_UPDATE:
                    if data["version"] > self.version:
                        print(f"[MQ] Received {data['type']} from peer {data['from']}.")
                        if self.version == 0:
                            print("[MQ] Initial state synchronization complete.")
                            self.ready_event.set()

                        self.version = data["version"]
                        self.peers = data["state"]
                        self.checkForChanges()

                    elif data["version"] < self.version:
                        print(f"[MQ] Received outdated {data['type']} from peer {data['from']}. Sending them our state")
                        self.publishState()
                    else:
                        print(f"[MQ] Received {data['type']} with same version from peer {data['from']}.")
                        continue
                
                elif data["type"] == DEPARTURE_NOTICE:
                    print(f"[MQ] Received {data['type']} from peer {data['from']}.")
                    del self.peers[data["virtual_ip"]]
                    self.version += 1
                    self.selfFix() # so i dont add back deleted peers by other modules in ALL sync
                    self.checkForChanges()

                elif data["type"] == ONBOARD_NOTICE:
                    print(f"[MQ] Received {data['type']} from peer {data['from']}.")
                    self.publishState()

                
            except zmq.Again:
                # # print(f"[MQ] zmq.Again")
                # self_fix_cnt += 1
                # if self_fix_cnt >= 2: # every 20s
                #     self_fix_cnt = 0
                #     self.selfFix()
                await asyncio.sleep(self.interval)

    def selfFix(self) -> None:
        for peer_ip in self.peers.keys():
            if peer_ip == self.state.ip:
                continue
            if peer_ip not in self.state.peers.keys():
                print(f"[MQ] Self-fix: Removing peer {peer_ip} which is not in state peers")
                del self.peers[peer_ip]

    def publishChange(self, virtual_ip: str, public_key: str, endpoint_ip: str, endpoint_port: int, sync_port: int | None = None) -> None:
        self.peers[virtual_ip] = {
            "virtual_ip": virtual_ip,
            "public_key": public_key,
            "endpoint_ip": endpoint_ip,
            "endpoint_port": endpoint_port,
        }
        if sync_port:
            self.peers[virtual_ip]["sync_port"] = sync_port
            self.sub.connect(f"tcp://{virtual_ip}:{sync_port}")
            print(f"[MQ] SUB connecting to tcp://{virtual_ip}:{sync_port}")
        self.version += 1
        self.publishState()

    def publishOnboard(self) -> None:
        msg = {
            "type": ONBOARD_NOTICE,
            "from": f"{self.state.ip}:{self.port}",
        }
        self.pub.send_string(json.dumps(msg))

    def publishState(self) -> None:
        msg = {
            "type": STATE_UPDATE,
            "from": f"{self.state.ip}:{self.port}",
            "version": self.version,
            "state": self.peers
        }
        
        self.pub.send_string(json.dumps(msg))

    def createTask(self, awaitable):
        return asyncio.run_coroutine_threadsafe(awaitable, self.loop)
    
    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def initSync(self) -> None:
        print("Initializing Message Queue synchronization...")
        pass

    def getInfo(self) -> dict:
        info = {
            "sync-type": "MQ",
            "sync-seed": self.peers[self.state.ip],
        }
        return info

    def checkForChanges(self) -> None:

        # print("[MQ] BEFORE LOCK")
        self.state.lock_aquire(self)
        # print(f"[MQ] Checking for changes in peers {self.peers.keys()} vs state peers {self.state.peers.keys()}...")

        for peer_ip, peer_info in self.peers.items():
            if peer_ip == self.state.ip:
                continue
            existing_peer = self.state.peers.get(peer_ip)
            if not existing_peer:
                self.state.add_peer(
                    peer_info["virtual_ip"],
                    peer_info["public_key"],
                    peer_info["endpoint_ip"],
                    peer_info["endpoint_port"]
                )
                self.sub.connect(f"tcp://{peer_info['virtual_ip']}:{peer_info['sync_port']}")
                print(f"[MQ] Added new peer: {peer_ip}")

            else:
                if self.check_individual_peer_change(peer_info, existing_peer):
                    print(f"[MQ] Detected change in peer {peer_ip}")

        existing_peers_copy = list(self.state.peers.items())
        for existing_peer_ip, existing_peer_info in existing_peers_copy:
            if existing_peer_ip not in self.peers.keys():
                self.state.remove_peer(existing_peer_ip)
                print(f"[MQ] Removed peer: {existing_peer_ip}")

        self.state.lock_release()

    def publishLastMessage(self) -> None:
        msg = {
            "type": DEPARTURE_NOTICE,
            "from": f"{self.state.ip}:{self.port}",
            "virtual_ip": self.state.ip
        }
        print(f"[MQ] Publishing departure notice")
        self.pub.send_string(json.dumps(msg))
        time.sleep(1) # give some time for message to be sent before shutting down sockets
        


    def exitSync(self) -> None:
        print(f"[MQ] Shutting down Message Queue synchronization...")
        self.publishLastMessage()
        self.terminate_event.set()
        self.listen_task.cancel()
        self.sub.close()
        self.sub_context.term()
        self.pub.close()
        self.pub_context.term()