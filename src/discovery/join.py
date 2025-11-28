from .base import DiscoveryBase
import socket
import threading
from sync.dht import SyncDHT
from state import State

class DiscoveryJoin(DiscoveryBase):
    def __init__(self, injected_state: State, injected_sync: SyncDHT, bootstrap_port = 17777):
        self.state = injected_state
        self.sync = injected_sync
        self.bootstrap_port = bootstrap_port
        self.running = True

    def startAccept(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print(f"Binding to port {self.bootstrap_port} for accepting JOIN connections...")
        sock.bind(("", self.bootstrap_port))
        
        sock.listen(5)
        while self.running:
            print(f"Listening on bootstrap port {self.bootstrap_port} for JOIN connections...")
            client, addr = sock.accept() 
            print(f"[+] Accepted connection from: {addr[0]}:{addr[1]}")
            
            client_handler = threading.Thread(target=self.handle_client, args=(client,))
            client_handler.start() 

            self.running = False  # Accept only one connection for JOIN

        sock.close()

    def handle_client(self, client):
        request = client.recv(1024)
        print(f"[*] Received: {request.decode('utf-8')}")

        dict_msg = eval(request.decode('utf-8'))

        msg = {}

        if dict_msg["content"]['ip'] in self.state.peers or dict_msg["content"]['ip'] == self.state.ip:
            print(f"Peer with IP {dict_msg['content']['ip']} already exists. Skipping addition.")
            msg = {
                "type": "ERROR",
                "status": "exists"
            }
            
        elif dict_msg["type"] != "JOIN":
            print(f"Invalid discovery type: {dict_msg['type']}. Expected 'JOIN'.")
            msg = {
                "type": "ERROR",
                "status": "invalid_type"
            }
        else:
            print(f"Adding new peer with IP {dict_msg['content']['ip']}:{dict_msg['content']['port']}")
            self.state.add_peer(
                dict_msg["content"]["ip"],
                dict_msg["content"]["public_key"],
                dict_msg["content"]["public_ip"],
                dict_msg["content"]["port"]
            )
            msg = {
                "type": "JOIN",
                "status": "success",
                "content": self.state.interface_json(),
                "sync": self.sync.getInfo() 
            }

            
            self.sync.publishChange(
                dict_msg["content"]['ip'],
                dict_msg["content"]['public_key'],
                dict_msg["content"]['public_ip'],
                dict_msg["content"]['port']
            )
            self.state.reload_config()
            
            

        client.send(str(msg).encode('utf-8'))
        client.close()

    def startJoin(self, bootstrap_node: str) -> dict:
        print(f"Joining the network via bootstrap node: {bootstrap_node}")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((bootstrap_node, self.bootstrap_port))
            msg = {
                "type": "JOIN",
                "status": "request",
                "content": self.state.interface_json()
            }
            sock.send(str(msg).encode('utf-8'))
            response = sock.recv(4096)
            print(f"[*] Received: {response.decode('utf-8')}")
            dict_msg = eval(response.decode('utf-8'))
            if dict_msg["type"] == "ERROR":
                print(f"Error during JOIN: {dict_msg['status']}")
            else:
                print("JOIN successful.")

            self.state.add_peer(
                dict_msg["content"]["ip"],
                dict_msg["content"]["public_key"],
                dict_msg["content"]["public_ip"],
                dict_msg["content"]["port"]
            )

            return dict_msg["sync"]

            