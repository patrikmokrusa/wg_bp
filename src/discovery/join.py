# Autor: Patrik Mokruša (xmokrup00)
from ast import literal_eval
from .base import DiscoveryBase
import socket
import threading
from sync.dht import SyncDHT
from sync.gossip import SyncGossip
from sync.mq import MessageQueueSync
from state import State

class DiscoveryJoin(DiscoveryBase):
    """
    Direct join discovery module. Nodes can connect to a bootstap node via TCP and exchange initial information. 
    """
    def __init__(self, injected_state: State, injected_sync: SyncDHT | SyncGossip | MessageQueueSync, bootstrap_port: int = 17777):
        """ Constructor for the DiscoveryJoin class. Initializes the state, synchronization module, and bootstrap port. """
        self._state = injected_state
        self._sync = injected_sync
        self._bootstrap_port = bootstrap_port
        self._running = True

    def getInfo(self) -> dict:
        """ Returns information about the discovery method. Used for dnssd discovery purposes. """
        return {
            "type": "JOIN",
            "port": self._bootstrap_port
        }

    def stopAccept(self) -> None:
        """ Stops the accept loop and closes the socket. """
        self._running = False
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()

    def startAccept(self) -> None:
        """ Starts the accept loop in a separate thread. """
        self.thread = threading.Thread(target=self._acceptLoop, daemon=True)
        self.thread.start()

    def _acceptLoop(self) -> None:
        """ Accept loop to listen for incoming JOIN connections and handle them. """
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        print(f"[JOIN] Binding to port {self._bootstrap_port} for accepting JOIN connections...")
        self.socket.bind(("", self._bootstrap_port))
        
        self.socket.listen()
        while self._running:
            print(f"[JOIN] Listening on bootstrap port {self._bootstrap_port} for JOIN connections...")
            try:
                client, addr = self.socket.accept() 
            except Exception:
                print(f"[JOIN] Socket closed, stopping accept loop.")
                return
            print(f"[JOIN] Accepted connection from: {addr[0]}:{addr[1]}")
            
            client_handler = threading.Thread(target=self._handle_client, args=(client,))
            client_handler.start() 

        self.socket.close()


    def _get_error_msg(self, status: str) -> dict:
        """ Helper function to create an error message response. """
        return {
            "type": "ERROR",
            "status": status
        }
    
    def _parse_request_msg(self, msg_str: str) -> tuple:
        """ Helper function to parse a request message from a string. """
        msg = literal_eval(msg_str)
        type = msg["type"]
        status = msg["status"]
        content = msg["content"]

        # check keys
        required_keys = {"ip", "public_key", "public_ip", "port"}
        if not required_keys.issubset(content.keys()):
            raise ValueError("Missing required keys in JOIN content")
        return type, status, content


    def _handle_client(self, client: socket.socket) -> None:
        """ Handles an incoming JOIN connection from a client. Parses the request, updates state, and sends a response. """
        request = client.recv(1024)
        print(f"[JOIN] Received: {request.decode('utf-8')}")

        type, status, content = self._parse_request_msg(request.decode('utf-8'))

        response = {}

        if type != "JOIN":
            print(f"[JOIN] Invalid discovery type: {type}. Expected 'JOIN'.")
            response = self._get_error_msg("invalid_type")
            client.send(str(response).encode('utf-8'))
        elif content['ip'] in self._state.peers or content['ip'] == self._state.ip:
            print(f"[JOIN] Peer with IP {content['ip']} already exists. Skipping addition.")
            response = self._get_error_msg("exists")
            client.send(str(response).encode('utf-8'))
            
        else:
            print(f"[JOIN] Adding new peer with IP {content['ip']}:{content['port']}")


            response = {
                "type": "JOIN",
                "status": "success",
                "content": self._state.interface_json(),
                "sync": self._sync.getInfo() 
            }
            
            client.send(str(response).encode('utf-8'))
            # ip, port = self._state.updatePeerAfterHandshake(content["ip"])
            self._state.lock_aquire(self)
            self._state.add_peer(
                content["ip"],
                content["public_key"],
                content["public_ip"],
                content["port"]
            )
            
            client.recv(1024)

            self._sync.publishChange(
                content['ip'],
                content['public_key'],
                # ip,
                # port,
                content['public_ip'],
                content['port'],
                sync_port=content['sync_port']
            )
            
            client.send(b"OK")  # confirmation because gossip
            self._state.lock_release()

        client.close()

    def _parse_response_msg(self, msg_str: str) -> tuple:
        """ Helper function to parse a response message from a string. """
        msg = literal_eval(msg_str)
        type = msg["type"]
        status = msg["status"]
        content = msg["content"]
        sync = msg["sync"]
        return type, status, content, sync

    def startJoin(self, bootstrap_node: str, sync_port: int = None) -> dict:
        """ Connects to a bootstrap node to join the network. Sends a JOIN request and waits for a response with the current state and synchronization information. """
        print(f"[JOIN] Joining the network via bootstrap node: {bootstrap_node}")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((bootstrap_node, self._bootstrap_port))
            content = self._state.interface_json()
            content["sync_port"] = sync_port
            msg = {
                "type": "JOIN",
                "status": "request",
                "content": content
            }
            sock.send(str(msg).encode('utf-8'))
            response = sock.recv(4096)
            print(f"[JOIN] Received response.")
            type, status, content, sync = self._parse_response_msg(response.decode('utf-8'))
            if type == "ERROR":
                print(f"[JOIN] Error during JOIN: {status}")
            else:
                print(f"[JOIN] JOIN successful.")

            self._state.add_peer(
                content["ip"],
                content["public_key"],
                content["public_ip"],
                content ["port"]
            )
            sock.send(b"OK")
            ok = sock.recv(1024)  # wait for confirmation because gossip
            print(f"[JOIN] Received confirmation: {ok.decode('utf-8')}")

            return sync

            