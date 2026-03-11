from ast import literal_eval
from .base import DiscoveryBase
import socket
import threading
from sync.dht import SyncDHT
from sync.gossip import SyncGossip
from sync.mq import MessageQueueSync
from state import State

class DiscoveryJoin(DiscoveryBase):
    def __init__(self, injected_state: State, injected_sync: SyncDHT | SyncGossip | MessageQueueSync, bootstrap_port = 17777):
        self.state = injected_state
        self.sync = injected_sync
        self.bootstrap_port = bootstrap_port
        self.running = True

    def getInfo(self):
        return {
            "type": "JOIN",
            "port": self.bootstrap_port
        }

    def stopAccept(self):
        self.running = False
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()

    def startAccept(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print(f"Binding to port {self.bootstrap_port} for accepting JOIN connections...")
        self.socket.bind(("", self.bootstrap_port))
        
        self.socket.listen()
        while self.running:
            print(f"Listening on bootstrap port {self.bootstrap_port} for JOIN connections...")
            try:
                client, addr = self.socket.accept() 
            except Exception:
                print("Socket closed, stopping accept loop.")
                return
            print(f"[+] Accepted connection from: {addr[0]}:{addr[1]}")
            
            client_handler = threading.Thread(target=self.handle_client, args=(client,))
            client_handler.start() 

        self.socket.close()


    def get_error_msg(self, status):
        return {
            "type": "ERROR",
            "status": status
        }
    
    def parse_request_msg(self, msg_str):
        msg = literal_eval(msg_str)
        type = msg["type"]
        status = msg["status"]
        content = msg["content"]

        # check keys
        required_keys = {"ip", "public_key", "public_ip", "port"}
        if not required_keys.issubset(content.keys()):
            raise ValueError("Missing required keys in JOIN content")
        return type, status, content


    def handle_client(self, client):
        request = client.recv(1024)
        print(f"[*] Received: {request.decode('utf-8')}")

        type, status, content = self.parse_request_msg(request.decode('utf-8'))

        response = {}

        if type != "JOIN":
            print(f"Invalid discovery type: {type}. Expected 'JOIN'.")
            response = self.get_error_msg("invalid_type")
        elif content['ip'] in self.state.peers or content['ip'] == self.state.ip:
            print(f"Peer with IP {content['ip']} already exists. Skipping addition.")
            response = self.get_error_msg("exists")
            
        else:
            
            print(f"Adding new peer with IP {content['ip']}:{content['port']}")
            self.state.add_peer(
                content["ip"],
                content["public_key"],
                content["public_ip"],
                content["port"]
            )

            self.state.reload_config()

            response = {
                "type": "JOIN",
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
            
        client.send(str(response).encode('utf-8'))
        client.close()

    def parse_response_msg(self, msg_str):
        msg = literal_eval(msg_str)
        type = msg["type"]
        status = msg["status"]
        content = msg["content"]
        sync = msg["sync"]
        return type, status, content, sync

    def startJoin(self, bootstrap_node: str, sync_port: int = None) -> dict:
        print(f"Joining the network via bootstrap node: {bootstrap_node}")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((bootstrap_node, self.bootstrap_port))
            content = self.state.interface_json()
            content["sync_port"] = sync_port
            msg = {
                "type": "JOIN",
                "status": "request",
                "content": content
            }
            sock.send(str(msg).encode('utf-8'))
            response = sock.recv(4096)
            print(f"[*] Received: {response.decode('utf-8')}")
            type, status, content, sync = self.parse_response_msg(response.decode('utf-8'))
            if type == "ERROR":
                print(f"Error during JOIN: {status}")
            else:
                print("JOIN successful.")

            self.state.add_peer(
                content["ip"],
                content["public_key"],
                content["public_ip"],
                content ["port"]
            )

            return sync

            