import socket
import json


import subprocess
import time
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

CUSTOM_STUN_PORT = 9999

CUSTOM_STUN_SERVERS = [
    ("172.20.0.10", CUSTOM_STUN_PORT), # TRUELY NATED DOCKER COMPOSE
    # ("stun", CUSTOM_STUN_PORT), # stun container in same network
    ("host.docker.internal", CUSTOM_STUN_PORT), # stun container
    # ("127.0.0.1", CUSTOM_STUN_PORT), # localhost
    ("172.18.0.1", CUSTOM_STUN_PORT), # docker no fw
    ("10.10.2.104", CUSTOM_STUN_PORT), # host machine
]

class State:
    def __init__(self, ip: str, port: int = 51820, interface: str = "wg0", keepalive: int = 25, forwarded_port: int | None = None) -> None:
        self.private_key = None
        self._gen_private_key()
        self.public_key = None
        self._gen_public_key()
        self.ip = ip
        self.port = port
        self.forwarded_port = forwarded_port
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

        self.ipr.link("set", index=idx, mtu=1420, state="up")

    
    def _wgInit(self) -> None:
        self.wg = WireGuard()

        self.wg.set(
            self.interface,
            private_key=self.private_key,
            listen_port=self.port
        )

    def updatePeerAfterHandshake(self, virtual_ip: str) -> tuple:
        wg = WireGuard()
        # print(f"[*****STATE] Waiting for handshake completion with peer {virtual_ip}")

        wait = True
        while wait:
            info = wg.info(self.interface)
            attrs = info[0]['attrs']
            attrs = dict(attrs)
            
            peers = attrs['WGDEVICE_A_PEERS']
            for peer in peers:
                peer_attrs = dict(peer['attrs'])
                if f"{virtual_ip}/32" in peer_attrs["WGPEER_A_ALLOWEDIPS"][0]['addr']:
                    if peer_attrs["WGPEER_A_RX_BYTES"] == 0:
                        print(f"[STATE] Handshake with peer {virtual_ip} not completed yet. Waiting...")
                        time.sleep(0.1)
                        continue
                    endpoint = peer_attrs['WGPEER_A_ENDPOINT']
                    addr = endpoint['addr']
                    port = endpoint['port']
                    # self.peers[virtual_ip]["endpoint_ip"] = addr
                    # self.peers[virtual_ip]["endpoint_port"] = port
                    wait = False
                    print(f"[STATE] Updated peer {virtual_ip} endpoint to {endpoint['addr']}:{endpoint['port']}")
                    break

        wg.close()

        return addr, port

            


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
            print(f"[STATE] Trying custom STUN server {stun_host}:{stun_port}...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(3)
            sock.bind(("0.0.0.0", self.port))
            try:
                sock.sendto(b"STUN request", (stun_host, stun_port))
                data, addr = sock.recvfrom(1024)
                print(f"[STATE] Received STUN response: {data.decode('utf-8')} from {addr[0]}:{addr[1]}")
                response = json.loads(data.decode('utf-8'))
                self.public_ip = response['ip']
                if self.forwarded_port:
                    self.public_port = self.forwarded_port
                else:
                    self.public_port = response['port']
                sock.close()
                return
            except Exception as e:
                sock.close()
                pass
        
        print(f"[STATE] Custom STUN failed.")

        try:
            self.public_ip, self.public_port = self.get_public_ip()
            if self.forwarded_port:
                self.public_port = self.forwarded_port  
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
        # self.ping_all_peers()

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


    def ping_all_peers(self)-> None:
        for peer_ip in self.peers.keys():
            print(f"[STATE] Pinging peer {peer_ip}")
            ping(peer_ip, verbose=True, count=2, timeout=0.5)

    
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

    


    def interface_json(self)-> dict:
        return {
            "ip": self.ip,
            "port": self.public_port,
            "public_key": self.public_key,
            "public_ip": self.public_ip
        }
        