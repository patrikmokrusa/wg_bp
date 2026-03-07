import socket
import threading
from state import State
from sync.dht import SyncDHT
from sync.gossip import SyncGossip
from sync.mq import MessageQueueSync
import json

JOIN_REQUEST = "BCAST_JOIN_REQUEST"
JOIN_RESPONSE = "BCAST_JOIN_RESPONSE"
ERROR  = "ERROR"

class DiscoveryBroadcast:
    def __init__(self, injected_state: State, injected_sync: SyncDHT | SyncGossip | MessageQueueSync | None, bootstrap_port = 18888):
        self.state = injected_state
        self.sync = injected_sync
        self.bootstrap_port = bootstrap_port
        self.running = True
        self.thread = None
        self.socket = None

    def stopAccept(self):
        self.running = False
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()

    def startAccept(self):
        self.thread = threading.Thread(target=self._broadcastAcceptLoop, daemon=True)
        self.thread.start()

    def _broadcastAcceptLoop(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("", self.bootstrap_port))
        print(f"[BCAST] Listening for broadcast messages on port {self.bootstrap_port}...")
        while self.running:
            try:
                data, addr = self.socket.recvfrom(1024)
            except Exception as e:
                print(f"[BCAST] Socket closed, stopping broadcast accept loop. error: {e}")
                return
            
            if self.running:
                print(f"[BCAST] Received broadcast message from {addr[0]}:{addr[1]}")
                self._handle_client(data, addr)

    def _handle_client(self, data, addr):
        request = json.loads(data.decode('utf-8'))
        print(f"[BCAST] Received message: {request} from {addr[0]}:{addr[1]}")
        if request['type'] == JOIN_REQUEST:
            print(f"[BCAST] Received JOIN request from {addr[0]}:{addr[1]}")
            content = request['content']

            if content['ip'] in self.state.peers or content['ip'] == self.state.ip:
                print(f"Peer with IP {content['ip']} already exists. Skipping addition.")
                response = {
                    "type": ERROR,
                    "status": "Peer with this IP already exists"
                }
            else: 

                self.state.add_peer(
                    content["ip"],
                    content["public_key"],
                    content["public_ip"],
                    content["port"]
                    )
                
                self.state.reload_config()

                response = {
                    "type": JOIN_RESPONSE,
                    "status": "success",
                    "content": self.state.interface_json(),
                    "sync": self.sync.getInfo() 
                }                
                self.sync.publishChange(
                    content['ip'],
                    content['public_key'],
                    content['public_ip'],
                    content['port'],
                    content['sync_port']
                )
        else:
            response = {
                "type": ERROR,
                "status": "Invalid request type"
            }

        self.socket.sendto(json.dumps(response).encode('utf-8'), addr)

    def startJoin(self, bootstrap_node: str = None, sync_port: int = None):
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        # client.settimeout(5)

        content = self.state.interface_json()
        content["sync_port"] = sync_port
        msg = {
            "type": JOIN_REQUEST,
            "status": "request",
            "content": content
        }

        try:
            client.sendto(json.dumps(msg).encode('utf-8'), ("<broadcast>", self.bootstrap_port))

            data, addr = client.recvfrom(1024)
            response = json.loads(data.decode('utf-8'))
            print(f"[BCAST] Received response from {addr[0]}:{addr[1]}: {response}")

            if response["type"] != JOIN_RESPONSE:
                print(f"[BCAST] Received error response: {response['status']}")

            content = response["content"]

            self.state.add_peer(
                content["ip"],
                content["public_key"],
                content["public_ip"],
                content ["port"]
            )

        except Exception as e:
            print(f"[BCAST] Error occurred while sending join request. error: {e}")
        finally:
            client.close()
        
        return response["sync"]