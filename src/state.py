import socket
import json


import subprocess
import urllib
import stun
from pythonping import ping
from pyroute2 import IPRoute, WireGuard
import threading
import asyncio

STUN_SERVERS = [
    ("stun.l.google.com", 19302),
    ("stunserver2025.stunprotocol.org", 3478),
    ("stun1.l.google.com", 19302),
]

CUSTOM_STUN_SERVERS = [
    ("stun", 9999),
    ("host.docker.internal", 9999),
    ("127.0.0.1", 9999),
    ("172.18.0.1", 9999),
]

class State:
    def __init__(self, ip: str, port: int = 51820, interface: str = "wg0", keepalive: int = 25) -> None:
        self.private_key = None
        self._gen_private_key()
        self.public_key = None
        self._gen_public_key()
        self.ip = ip
        self.port = port
        self.peers = {} # peer_virtual_ip: {public_key : key_str, endpoint_ip : endpoint_str}
        self.interface = interface
        self.keepalive = keepalive
        self.bootstrap_peer = None
        self.public_ip = None
        self.public_port = None
        self.update_public_ip()
        self._iplinkInit()
        self.lock = threading.Lock()

    def lock_aquire(self, requester) -> None:
        # print(f"[STATE] {requester} acquiring lock...")
        self.lock.acquire()

    def lock_release(self) -> None:
        self.lock.release()

    def _iplinkInit(self) -> None:
        self.ipr = IPRoute()

        if not self.ipr.link_lookup(ifname=self.interface):
            self.ipr.link("add", ifname=self.interface, kind="wireguard")

        idx = self.ipr.link_lookup(ifname=self.interface)[0]

        self.ipr.addr("add", index=idx, address=self.ip, prefixlen=24)

        self._wgInit()

        self.ipr.link("set", index=idx, state="up")

    
    def _wgInit(self) -> None:
        self.wg = WireGuard()

        self.wg.set(
            self.interface,
            private_key=self.private_key,
            listen_port=self.port
        )

    def _wg_set(self, interface, **kwargs) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            self.wg.set(interface, **kwargs)
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            asyncio.set_event_loop(None)
            loop.close()

    def _wgSetOwnEventLoop(self, interface, **kwargs) -> None:
        thread = threading.Thread(target=self._wg_set, args=(interface,), kwargs=kwargs)
        thread.start()
        thread.join()

    def get_public_ip(self):
        print("[STATE] Determining public IP and port via STUN...")

        for stun_host, stun_port in STUN_SERVERS:
            try:
                mapped_addr = stun.get_ip_info(
                    '0.0.0.0',
                    self.port,
                    stun_host=stun_host,
                    stun_port=stun_port,
                )
                print(f"[STATE] STUN result from {stun_host}:{stun_port}: {mapped_addr}")

                if mapped_addr[1] is None or mapped_addr[2] is None:
                    continue

                return mapped_addr[1], mapped_addr[2]
                if mapped_addr[0] == 'Symmetric NAT':
                    print("[STATE] Symmetric NAT detected. Direct UDP hole punching may fail.")
                return
            except Exception as e:
                print(f"[STATE] STUN lookup failed via {stun_host}:{stun_port}: {e}")


    def update_public_ip(self) -> None:
        print("[STATE] CUSTOM STUN")

        for stun_host, stun_port in CUSTOM_STUN_SERVERS:
                
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(3)
            sock.bind(("", self.port))
            try:
                sock.sendto(b"STUN request", (stun_host, stun_port))
                data, addr = sock.recvfrom(1024)
                print(f"[STATE] Received STUN response: {data.decode('utf-8')} from {addr[0]}:{addr[1]}")
                response = json.loads(data.decode('utf-8'))
                self.public_ip = response['ip']
                self.public_port = response['port']
                sock.close()
                return
            except Exception as e:
                pass
        
        print(f"[STATE] Custom STUN failed.")
        sock.close()

        try:
            self.public_ip, self.public_port = self.get_public_ip()
        except Exception as e:
            print(f"[STATE] Error occurred while fetching public IP: {e}")
            exit(1)

    def _gen_private_key(self)-> None:
        cli = subprocess.Popen(["wg", "genkey"], stdout=subprocess.PIPE)
        key = cli.stdout.read().decode("utf-8")
        self.private_key = key.rstrip("\n")
        # self.private_key = key

    
    def _gen_public_key(self)-> None:
        cli = subprocess.Popen(["wg", "pubkey"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        key = cli.communicate(input=self.private_key.encode("utf-8"))[0].decode("utf-8")
        self.public_key = key.rstrip("\n")
        # self.public_key = key

    def add_peer(self, peer_virtual_ip: str, public_key: str, endpoint_ip: str, endpoint_port: int = 51820) -> None:
        if peer_virtual_ip == self.ip:
            return
        self.peers[peer_virtual_ip] = {"public_key": public_key, "endpoint_ip": endpoint_ip, "endpoint_port": endpoint_port}

        self._wgSetOwnEventLoop(
            self.interface,
            peer={
                "public_key": public_key.strip(),
                "allowed_ips": self._getAllowedIPs(peer_virtual_ip),
                "endpoint_addr": endpoint_ip,
                "endpoint_port": endpoint_port,
                "persistent_keepalive": self.keepalive
            }
        )
        print(self.get_config())
        print(f"[STATE] Added peer {peer_virtual_ip}")
        self.ping_all_peers()

    def _getAllowedIPs(self, peer_virtual_ip: str) -> list:
        allowed_ips = []
        if "/" in peer_virtual_ip:
            allowed_ips.append(peer_virtual_ip)
        else:
            # Each peer should own only its host address in WireGuard cryptokey routing.
            allowed_ips.append(peer_virtual_ip + "/32")
        return allowed_ips


    def remove_peer(self, peer_virtual_ip: str) -> None:
        if peer_virtual_ip in self.peers:

            try:
                self._wgSetOwnEventLoop(
                    self.interface,
                    peer={
                        "public_key": self.peers[peer_virtual_ip]["public_key"].strip(),
                        "remove": True
                    }
                )
            except Exception as e:
                print(f"[STATE] Error removing peer from WireGuard config: {e}")

            del self.peers[peer_virtual_ip]

            print(self.get_config())
            print(f"[STATE] Removed peer {peer_virtual_ip}")


    def get_config(self)-> str:
        config = "\n"
        config += "[Interface]\n"
        config += f"PrivateKey = {self.private_key}\n"
        config += f"Address = {self.ip}\n"
        config += f"ListenPort = {self.port}\n\n"
        for peer_ip, peer_info in self.peers.items():
            config += "[Peer]\n"
            config += f"PublicKey = {peer_info['public_key']}\n"
            config += f"AllowedIPs = {peer_ip}\n"
            config += f"Endpoint = {peer_info['endpoint_ip']}:{peer_info['endpoint_port']}\n"
            config += f"PersistentKeepalive = {self.keepalive}\n\n"
        
        return config

    def write_config(self)-> None:
        return
        filename = f"/etc/wireguard/{self.interface}.conf"
        with open(filename  , "w") as f:
            f.write(self.get_config())

    def load_config(self)-> None:
        return
        print(self.get_config())
        subprocess.run(["wg-quick", "up", self.interface])
        self.ping_all_peers()

    def ping_all_peers(self)-> None:
        for peer_ip in self.peers.keys():
            ping(peer_ip, verbose=False, count=3, timeout=0)

    def disable_config(self)-> None:
        return
        subprocess.run(["wg-quick", "down", self.interface])
    
    def disableNetlink(self):
        idx = self.ipr.link_lookup(ifname=self.interface)[0]
        # self.ipr.link("set", index=idx, state="down")
        self.ipr.link("delete", index=idx)

        self.ipr.close()
        self.wg.close()

    def netlinkUp(self):
        idx = self.ipr.link_lookup(ifname=self.interface)[0]
        self.ipr.link("set", index=idx, state="up")

    def netlinkDown(self):
        idx = self.ipr.link_lookup(ifname=self.interface)[0]
        self.ipr.link("set", index=idx, state="down")

    

    def reload_config(self)-> None:
        return
        #TODO: use wg strip
        self.disable_config()
        self.write_config()
        self.load_config()

    def interface_json(self)-> dict:
        return {
            "ip": self.ip,
            "port": self.public_port,
            "public_key": self.public_key,
            "public_ip": self.public_ip
        }
        