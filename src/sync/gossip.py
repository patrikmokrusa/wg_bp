from ast import literal_eval
import asyncio
import threading

from state import State
from .base import SyncBase

from libp2p import new_host
from libp2p.pubsub import gossipsub
from libp2p.peer.peerinfo import info_from_p2p_addr
from multiaddr import Multiaddr

TOPIC = "GossipSyncTopic"

MSG_ADD_PEER = "ADD_PEER"
MSG_UPDATE_PEER = "UPDATE_PEER"
MSG_REMOVE_PEER = "REMOVE_PEER"


class SyncGossip(SyncBase):
    def __init__(self, injected_state : State, seed_node=None, port=6888):
        print("Initializing Gossip synchronization...")
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        self.loop_thread.start()

        self.state = injected_state
        self.seed_node = seed_node
        self.port = port

        self.listenForChangesRunning = False
        self.sub = None

        self.host = new_host(listen_addrs=[Multiaddr(f"/ip4/{self.state.ip}/tcp/{self.port}")])
        gossip = gossipsub.GossipSub(["/meshsub/1.1.0"], 
                                     degree=8, degree_low=6, degree_high=12)
        self.pubsub = gossipsub.Pubsub(self.host, gossip)
        print(f"[*] Gossip host created on: {self.state.ip}:{self.port}")

        if seed_node:
            seed_info = info_from_p2p_addr(Multiaddr(seed_node))
            self.createTask(self.host.connect(seed_info)).result()
            print(f"[*] Connected to seed node: {seed_node}")
        
        self.createTask(self._listen_for_changes())

    def initSync(self):
        print("Initializing Gossip synchronization...")

    def getInfo(self):
        info = {
            "sync-type": "Gossip",
            "sync-ip": self.state.ip,
            "sync-port": self.port,
            "sync-id": str(self.host.get_id())
        }
        return info

    def publishChange(self, virtual_ip, public_key, endpoint_ip, endpoint_port):
        print("Publishing changes to Gossip network...")
        msg = {
            "type": MSG_ADD_PEER,
            "virtual_ip": virtual_ip,
            "public_key": public_key,
            "endpoint_ip": endpoint_ip,
            "endpoint_port": endpoint_port
        }
        self.createTask(self.pubsub.publish(TOPIC, str(msg).encode()))
    
    async def _listen_for_changes(self):
        print("Listening for changes from Gossip network...")
        self.listenForChangesRunning = True
        self.sub = await self.pubsub.subscribe(TOPIC)
        while self.listenForChangesRunning:
            msg = await self.sub.get()
            print(f"Received message from {msg.from_id}: {msg.data.decode()}")
            if msg.from_id == self.host.get_id():
                continue
            try:
                data = literal_eval(msg.data.decode())
                print(f"Received message from {msg.from_id}: {data}")
                if data["type"] == MSG_ADD_PEER:
                    self.state.add_peer(
                        data["virtual_ip"],
                        data["public_key"],
                        data["endpoint_ip"],
                        data["endpoint_port"]
                    )
                    print(f"Added peer from Gossip: {data['virtual_ip']} -> {data}")
                elif data["type"] == MSG_REMOVE_PEER:
                    self.state.remove_peer(data["virtual_ip"])
                    print(f"Removed peer from Gossip: {data['virtual_ip']}")
                elif data["type"] == MSG_UPDATE_PEER:
                    existing_peer = self.state.peers.get(data["virtual_ip"])
                    if existing_peer:
                        result = self.check_individual_peer_change(data, existing_peer)
                        if result:
                            self.state.remove_peer(data["virtual_ip"])
                            self.state.add_peer(
                                data["virtual_ip"],
                                data["public_key"],
                                data["endpoint_ip"],
                                data["endpoint_port"]
                            )
                            print(f"Updated peer from Gossip: {data['virtual_ip']} -> {data}")
            except Exception as e:
                print(f"Error processing message: {e}")

    def listenForChanges(self):
        # Now handled internally by _listen_for_changes during __init__
        pass

    def exitSync(self):
        print("Exiting Gossip synchronization...")
        
        msg = {
            "type": MSG_REMOVE_PEER,
            "virtual_ip": self.state.ip,
            "public_key": self.state.public_key,
            "endpoint_ip": self.state.public_ip,
            "endpoint_port": self.state.public_port
        }
        
        try:
            # Publish exit message
            self.createTask(self.pubsub.publish(TOPIC, str(msg).encode())).result(timeout=5)
            print("[*] Exit message published, waiting for propagation...")
            
            # Give network time to propagate the message to other peers
            threading.Event().wait(timeout=3)
            
            # Now stop listening
            self.listenForChangesRunning = False
            
            # Unsubscribe
            if self.sub:
                self.createTask(self.pubsub.unsubscribe(self.sub)).result(timeout=5)
            
            # Close the host
            self.createTask(self.host.close()).result(timeout=5)
        except Exception as e:
            print(f"Error during exit: {e}")
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.loop_thread.join(timeout=1)

    def createTask(self, awaitable):
        return asyncio.run_coroutine_threadsafe(awaitable, self.loop)

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()


    def checkForChanges(self):
        return 
        