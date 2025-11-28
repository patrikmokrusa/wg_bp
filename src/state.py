import subprocess
import urllib.request

class State:
    def __init__(self, ip, port = 51820, interface="wg0", keepalive=25):
        self.private_key = None
        self.__gen_private_key()
        self.public_key = None
        self.__gen_public_key()
        self.ip = ip
        self.port = port
        self.peers = {} # peer_virtual_ip: {public_key : key_str, endpoint_ip : endpoint_str}
        self.interface = interface
        self.keepalive = keepalive
        self.bootstrap_peer = None
        self.public_ip = None
        self.update_public_ip()

    def update_public_ip(self):
        external_ip = urllib.request.urlopen('https://api.ipify.org').read().decode('utf8')
        self.public_ip = external_ip

    def __gen_private_key(self):
        cli = subprocess.Popen(["wg", "genkey"], stdout=subprocess.PIPE)
        self.private_key = cli.stdout.read().decode("utf-8")
    
    def __gen_public_key(self):
        cli = subprocess.Popen(["wg", "pubkey"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        self.public_key = cli.communicate(input=self.private_key.encode("utf-8"))[0].decode("utf-8")

    def add_peer(self, peer_virtual_ip, public_key, endpoint_ip, endpoint_port=51820):
        if peer_virtual_ip == self.ip:
            return
        self.peers[peer_virtual_ip] = {"public_key": public_key, "endpoint_ip": endpoint_ip, "endpoint_port": endpoint_port}

    def remove_peer(self, peer_virtual_ip):
        if peer_virtual_ip in self.peers:
            del self.peers[peer_virtual_ip]

    def get_config(self):
        config = ""
        config += "[Interface]\n"
        config += f"PrivateKey = {self.private_key}"
        config += f"Address = {self.ip}\n"
        config += f"ListenPort = {self.port}\n\n"
        for peer_ip, peer_info in self.peers.items():
            config += "[Peer]\n"
            config += f"PublicKey = {peer_info['public_key']}\n"
            config += f"AllowedIPs = {peer_ip}\n"
            config += f"Endpoint = {peer_info['endpoint_ip']}:{peer_info['endpoint_port']}\n"
            config += f"PersistentKeepalive = {self.keepalive}\n\n"
        
        return config

    def write_config(self):
        filename = f"/etc/wireguard/{self.interface}.conf"
        with open(filename  , "w") as f:
            f.write(self.get_config())

    def load_config(self):
        print(self.get_config())
        subprocess.run(["wg-quick", "up", self.interface])

    def disable_config(self):
        subprocess.run(["wg-quick", "down", self.interface])

    def reload_config(self):
        #TODO: use wg strip
        self.disable_config()
        self.write_config()
        self.load_config()

    def interface_json(self):
        return {
            "ip": self.ip,
            "port": self.port,
            "public_key": self.public_key,
            "public_ip": self.public_ip
        }
        