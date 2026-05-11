# Autor: Patrik Mokruša (xmokrup00)
import socket
import threading
from state import State
from sync.dht import SyncDHT
from sync.gossip import SyncGossip
from sync.mq import MessageQueueSync
from .base import DiscoveryBase
import json

JOIN_REQUEST = "BCAST_JOIN_REQUEST"
""" Represents a broadcast join request message type. """
JOIN_RESPONSE = "BCAST_JOIN_RESPONSE"
""" Represents a broadcast join response message type. """
ERROR  = "ERROR"
""" Represents an error message type. """

class DiscoveryBroadcast(DiscoveryBase):
    """
    Broadcast discovery module. Nodes can broadcast their presence on the local network or listen for other nodes
    """
    def __init__(self, injected_state: State, injected_sync: SyncDHT | SyncGossip | MessageQueueSync | None, bootstrap_port: int = 18888)-> None:
        """ Constructor for the DiscoveryBroadcast class. Initializes the state, synchronization module, and bootstrap port. """
        self._state = injected_state
        self._sync = injected_sync
        self._bootstrap_port = bootstrap_port
        self._running = True
        self._thread = None
        self._socket = None

    def getInfo(self) -> dict:
        """ Returns information about the discovery method. Used for dnssd discovery purposes. """
        return {
            "type": "BROADCAST",
            "port": self._bootstrap_port
        }

    def stopAccept(self) -> None:
        """ Stops listening and closes the socket. """
        self._running = False
        self._socket.close()
        

    def startAccept(self) -> None:
        """ Starts listening for broadcast messages in a separate thread. """
        self._thread = threading.Thread(target=self._broadcastAcceptLoop, daemon=True)
        self._thread.start()

    def _broadcastAcceptLoop(self) -> None:
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.settimeout(1)
        self._socket.bind(("", self._bootstrap_port))
        print(f"[BCAST] Listening for broadcast messages on port {self._bootstrap_port}...")
        while self._running:
            try:
                data, addr = self._socket.recvfrom(1024)
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[BCAST] Socket closed, stopping broadcast accept loop.")
                return
            
            if self._running:
                self._handle_client(data, addr)

    def _handle_client(self, data: bytes, addr: tuple) -> None:
        request = json.loads(data.decode('utf-8'))
        if request['type'] == JOIN_REQUEST:
            print(f"[BCAST] Received JOIN request from {addr[0]}:{addr[1]}")
            content = request['content']

            if content['ip'] in self._state.peers or content['ip'] == self._state.ip:
                print(f"Peer with IP {content['ip']} already exists. Skipping addition.")
                response = {
                    "type": ERROR,
                    "status": "Peer with this IP already exists"
                }
            else: 

                self._state.add_peer(
                    content["ip"],
                    content["public_key"],
                    content["public_ip"],
                    content["port"]
                    )
                

                response = {
                    "type": JOIN_RESPONSE,
                    "status": "success",
                    "content": self._state.interface_json(),
                    "sync": self._sync.getInfo() 
                }                
                self._sync.publishChange(
                    content['ip'],
                    content['public_key'],
                    content['public_ip'],
                    content['port'],
                    sync_port=content['sync_port']
                )
        else:
            response = {
                "type": ERROR,
                "status": "Invalid request type"
            }

        self._socket.sendto(json.dumps(response).encode('utf-8'), addr)

    def startJoin(self, bootstrap_node: str = None, sync_port: int = None) -> dict:
        """ Starts broadcasting a JOIN request to the local network to discover and join existing nodes. """
        print(f"[BCAST] Broadcasting JOIN request to local network...")
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        content = self._state.interface_json()
        content["sync_port"] = sync_port
        msg = {
            "type": JOIN_REQUEST,
            "status": "request",
            "content": content
        }

        try:
            client.sendto(json.dumps(msg).encode('utf-8'), ("<broadcast>", self._bootstrap_port))

            data, addr = client.recvfrom(1024)
            response = json.loads(data.decode('utf-8'))
            print(f"[BCAST] Received response from {addr[0]}:{addr[1]}: {response}")

            if response["type"] != JOIN_RESPONSE:
                print(f"[BCAST] Received error response: {response['status']}")

            content = response["content"]

            self._state.add_peer(
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