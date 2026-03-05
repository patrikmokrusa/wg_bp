import json
import threading
import zmq
import asyncio
import time
import socket
from .base import SyncBase

from state import State

STATE_UPDATE = "STATE_UPDATE"
DEPARTURE_NOTICE = "DEPARTURE_NOTICE"
ONBOARD_NOTICE = "ONBOARD_NOTICE"


class MessageQueueSync(SyncBase):
    def __init__(self, state: State, seed_node = None, port=5555, interval=0.1):
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

        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        self.loop_thread.start()
        self.createTask(self._async_init())

        if seed_node:
            print("[MQ] Waiting for initial state synchronization from seed node...")
            while self.version == 0:
                time.sleep(1) 
                self.publishOnboard()

    async def _async_init(self):
        self.terminate_event = asyncio.Event()
        self.listen_task = asyncio.create_task(self._listenForUpdates())
        

    async def _listenForUpdates(self):
        cnt = 0
        while not self.terminate_event.is_set():
            try:
                msg = self.sub.recv_string(flags=zmq.NOBLOCK)
                data = json.loads(msg)
                cnt += 1
                # print(f"[*] num:{cnt} Received message: {data['type']} from {data['from']}")
                if data["from"] == f"{self.state.ip}:{self.port}":
                    print("[MQ] Ignoring message from self")
                    continue


                if data["type"] == STATE_UPDATE:
                    if data["version"] > self.version:
                        print(f"[MQ] Received {data['type']} from peer {data['from']} via Message Queue...")
                        self.version = data["version"]
                        self.peers = data["state"]
                    elif data["version"] < self.version:
                        print(f"[MQ] Received outdated {data['type']} from peer {data['from']} via Message Queue... sending them our state")
                        self.publishState()
                    else:
                        print(f"[MQ] Received {data['type']} with same version from peer {data['from']} via Message Queue... no action needed")
                        # same version, no action needed
                        continue
                
                elif data["type"] == DEPARTURE_NOTICE:
                    print(f"[MQ] Received {data['type']} from peer {data['from']} via Message Queue...")
                    del self.peers[data["virtual_ip"]]
                    self.version += 1
                elif data["type"] == ONBOARD_NOTICE:
                    print(f"[MQ] Received {data['type']} from peer {data['from']} via Message Queue...")
                    self.publishState()

                self.checkForChanges()
            except zmq.Again:
                await asyncio.sleep(self.interval)
            # except Exception as e:
            #     raise e

    def publishChange(self, virtual_ip, public_key, endpoint_ip, endpoint_port, sync_port = None):
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

    def publishOnboard(self):
        msg = {
            "type": ONBOARD_NOTICE,
            "from": f"{self.state.ip}:{self.port}",
        }
        self.pub.send_string(json.dumps(msg))

    def publishState(self):
        # self.version += 1
        msg = {
            "type": STATE_UPDATE,
            "from": f"{self.state.ip}:{self.port}",
            "version": self.version,
            "state": self.peers
        }
        
        self.pub.send_string(json.dumps(msg))

    def createTask(self, awaitable):
        """Create a task in the event loop"""
        return asyncio.run_coroutine_threadsafe(awaitable, self.loop)
    
    def _run_loop(self):
        """Run the event loop"""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def initSync(self):
        print("Initializing Message Queue synchronization...")
        pass

    def getInfo(self):
        info = {
            "sync-type": "MQ",
            "sync-seed": self.peers[self.state.ip],
        }
        return info

    def checkForChanges(self):
        reload_required = False
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
                print(f"[MQ] Added new peer via MQ: {peer_ip} -> {peer_info}\n")
                print(self.state.get_config())
                reload_required = True
            else:
                print(f"[MQ] Checking peer for changes...")
                if self.check_individual_peer_change(peer_info, existing_peer):
                    print(f"Detected change in peer")
                    reload_required = True

        existing_peers_copy = list(self.state.peers.items())
        # print(f"[CHCK] existing_peers_copy: {existing_peers_copy}")
        for existing_peer_ip, existing_peer_info in existing_peers_copy:
            if existing_peer_ip not in self.peers.keys():
                self.state.remove_peer(existing_peer_ip)
                # self.sub.disconnect(f"tcp://{existing_peer_info['virtual_ip']}:{existing_peer_info['sync_port']}")
                print(f"[MQ] Removed peer via MQ: {existing_peer_ip} -> {existing_peer_info}\n")
                reload_required = True

        if reload_required:
            self.state.reload_config()
            print(f"[MQ] syn peers: {self.peers}")
            print(self.state.get_config())

    def publishLastMessage(self):
        msg = {
            "type": DEPARTURE_NOTICE,
            "from": f"{self.state.ip}:{self.port}",
            "virtual_ip": self.state.ip
        }
        print(f"[MQ] Publishing departure notice")
        self.pub.send_string(json.dumps(msg))


    def exitSync(self):
        print(f"[MQ] Shutting down Message Queue synchronization...")
        self.publishLastMessage()
        self.terminate_event.set()
        self.listen_task.cancel()
        self.pub.close()
        self.sub.close()
        self.pub_context.term()
        self.sub_context.term()