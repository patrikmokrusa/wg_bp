import socket
import json

import time
import stun
from pythonping import ping
from pyroute2 import IPRoute, WireGuard
import threading
import asyncio

import base64
from cryptography.hazmat.primitives.asymmetric import x25519
from cryptography.hazmat.primitives import serialization

STUN_SERVERS = [
    ("stun.l.google.com", 19302),
    ("stunserver2025.stunprotocol.org", 3478),
    ("stun1.l.google.com", 19302),
]
""" List of public STUN servers. """

CUSTOM_STUN_PORT = 9999
""" Port for custom STUN server."""


CUSTOM_STUN_SERVERS = [
    ("172.20.2.2", CUSTOM_STUN_PORT), # TRUELY NATED DOCKER COMPOSE test_sync
    ("stun", CUSTOM_STUN_PORT), # stun container in same network test_nat
    ("127.0.0.1", CUSTOM_STUN_PORT), # localhost
    # ("172.18.0.1", CUSTOM_STUN_PORT), # docker no fw
    # ("10.10.2.104", CUSTOM_STUN_PORT), # host machine
]
""" 
List of custom STUN servers for testing with a custom STUN server implemented in test/stun/stun.py.
Leave blank to skip custom STUN and use only public STUN servers. 
"""

class State:
    """
    Represents the actual state of the WireGuard interface and its peers.
    """
    def __init__(self, ip: str, port: int = 51820, interface: str = "wg0", keepalive: int = 25, prefix: int = 24, forwarded_port: int | None = None) -> None:
        """ 
        Constructor for the State class. Creates key pair, gets public IP, initializes netlink and WireGuard interface. 
        
        Args:
            ip: The virtual IP address for the WireGuard interface.
            port: The port for the WireGuard interface. Default is 51820.
            interface: The name of the WireGuard interface. Default is "wg0".
            keepalive: The persistent keepalive for WireGuard peers. Default is 25.
            prefix: The subnet prefix length for the WireGuard interface. Default is 24.
            forwarded_port: The port forwarded to the WireGuard interface (If set overrides the Stun public port). Default is None.

        """
        self.private_key = None
        """ The private key for the WireGuard interface. """
        self.public_key = None
        """ The public key for the WireGuard interface. """
        self._gen_key_pair()
        self.ip = ip
        """ The virtual IP address for the WireGuard interface. """
        self.port = port
        """ The port for the WireGuard interface. """
        self._forwarded_port = forwarded_port
        self.peers = {} # peer_virtual_ip: {public_key : key_str, endpoint_ip : endpoint_str}
        """ Dictionary of peers in the network. Maps virtual IPs to their public keys and endpoint information. """
        self.interface = interface
        """ The name of the WireGuard interface. """
        self._keepalive = keepalive
        self.public_ip = None
        """ The public IP address determined by STUN. """
        self.prefix = prefix
        """ The subnet prefix length for the WireGuard interface. """
        self.public_port = None
        """ The public port determined by STUN. """
        self._update_public_ip()
        self._iplinkInit()
        self._lock = threading.Lock()

        self.allowed_ips = [f"{self.ip}/32"]

    def lock_aquire(self, requester) -> None:
        """ Acquires the state lock. Should be used when modifying the state to prevent collisions between different modules."""
        # print(f"[STATE] {requester} acquiring lock...")
        self._lock.acquire()

    def lock_release(self) -> None:
        """ Releases the state lock. Should be used when modifying the state to prevent collisions between different modules."""
        self._lock.release()

    def _iplinkInit(self) -> None:
        """ Initializes the WireGuard interface using pyroute2. """
        self.ipr = IPRoute()

        if not self.ipr.link_lookup(ifname=self.interface):
            self.ipr.link("add", ifname=self.interface, kind="wireguard")

        idx = self.ipr.link_lookup(ifname=self.interface)[0]

        self.ipr.addr("add", index=idx, address=self.ip, prefixlen=self.prefix)

        self._wgInit()

        self.ipr.link("set", index=idx, mtu=1420, state="up")

    
    def _wgInit(self) -> None:
        """ Initializes the WireGuard interface using pyroute2. """
        self.wg = WireGuard()

        self.wg.set(
            self.interface,
            private_key=self.private_key,
            listen_port=self.port
        )

    def _updatePeerAfterHandshake(self, virtual_ip: str) -> tuple:
        """ Not used. Waits for handshake completion with a peer and updates its endpoint information. """
        wg = WireGuard()

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
        """ Helper method to set WireGuard configuration in a separate event loop. """
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
        """ Helper method to set Wireguard in a seperate thread. """
        thread = threading.Thread(target=self._wg_set, args=(interface,), kwargs=kwargs)
        thread.start()
        thread.join()

    def _get_public_ip(self):
        """ Uses STUN to get public IP and port. """
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
            except Exception as e:
                print(f"[STATE] STUN lookup failed via {stun_host}:{stun_port}: {e}")


    def _update_public_ip(self) -> None:
        """ Updates the public IP and port using custom STUN. Can be configured in global variable CUSTOM_STUN_SERVERS. Used for testing with custom STUN server in test/stun/stun.py. """
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
                if self._forwarded_port:
                    self.public_port = self._forwarded_port
                else:
                    self.public_port = response['port']
                sock.close()
                return
            except Exception as e:
                sock.close()
                pass
        
        print(f"[STATE] Custom STUN failed.")

        try:
            self.public_ip, self.public_port = self._get_public_ip()
            if self._forwarded_port:
                self.public_port = self._forwarded_port  
        except Exception as e:
            print(f"[STATE] Error occurred while fetching public IP: {e}")
            exit(1)

    def _gen_key_pair(self) -> None:
        """ Generates wireguard key pair. """
        private = x25519.X25519PrivateKey.generate()
        public = private.public_key()

        private_raw = private.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )
        public_raw = public.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )

        self.private_key = base64.b64encode(private_raw).decode("ascii")
        self.public_key = base64.b64encode(public_raw).decode("ascii")


    def add_peer(self, peer_virtual_ip: str, public_key: str, endpoint_ip: str, endpoint_port: int = 51820) -> None:
        """ Adds a peer to the state and WireGuard configuration. """
        if peer_virtual_ip == self.ip:
            return
        self.peers[peer_virtual_ip] = {
            "public_key": public_key, 
            "endpoint_ip": endpoint_ip, 
            "endpoint_port": endpoint_port,
            "allowed_ips": self._getAllowedIPs(peer_virtual_ip)
            }

        self._wgSetOwnEventLoop(
            self.interface,
            peer={
                "public_key": public_key.strip(),
                "allowed_ips": self._getAllowedIPs(peer_virtual_ip),
                "endpoint_addr": endpoint_ip,
                "endpoint_port": endpoint_port,
                "persistent_keepalive": self._keepalive
            }
        )
        print(self.get_config())
        print(f"[STATE] Added peer {peer_virtual_ip}")

    def add_allowed_ip(self, allowed_ip: str) -> None:
        """ Adds an allowed IP for the local node. """
        if allowed_ip not in self.allowed_ips:
            self.allowed_ips.append(allowed_ip)
            print(f"[STATE] Added allowed IP {allowed_ip} to local node.")

    def remove_allowed_ip(self, allowed_ip: str) -> None:
        """ Removes an allowed IP for the local node. """
        if allowed_ip in self.allowed_ips:
            self.allowed_ips.remove(allowed_ip)
            print(f"[STATE] Removed allowed IP {allowed_ip} from local node.")

    def set_peer_AllowedIPs(self, peer_virtual_ip: str, allowed_ips: list) -> None:
        """ Updates the allowed IPs for a peer in the state and WireGuard configuration. """

        self.peers[peer_virtual_ip]["allowed_ips"] = allowed_ips

        self._wgSetOwnEventLoop(
            self.interface,
            peer={
                "public_key": self.peers[peer_virtual_ip]["public_key"].strip(),
                "allowed_ips": allowed_ips,
                "endpoint_addr": self.peers[peer_virtual_ip]["endpoint_ip"],
                "endpoint_port": self.peers[peer_virtual_ip]["endpoint_port"],
                "persistent_keepalive": self._keepalive
            }
        )
        print(f"[STATE] Updated peer {peer_virtual_ip} allowed IPs to {allowed_ips}")

    def _getAllowedIPs(self, peer_virtual_ip: str) -> list:
        """ Helper method to parse allowed IP for peers when initialy adding them. """
        allowed_ips = []
        if "/" in peer_virtual_ip:
            allowed_ips.append(peer_virtual_ip)
        else:
            # Each peer defaultly owns only its host address in WireGuard cryptokey routing.
            allowed_ips.append(peer_virtual_ip + "/32")
        return allowed_ips


    def remove_peer(self, peer_virtual_ip: str) -> None:
        """ Removes a peer from the state and WireGuard configuration. """
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
        """ Generates a WireGuard configuration file content based on the current state. Can be used for printing or WG-Quick."""
        config = "\n"
        config += "[Interface]\n"
        config += f"PrivateKey = {self.private_key}\n"
        config += f"Address = {self.ip}\n"
        config += f"ListenPort = {self.port}\n\n"
        for peer_ip, peer_info in self.peers.items():
            config += "[Peer]\n"
            config += f"PublicKey = {peer_info['public_key']}\n"
            config += f"AllowedIPs = {peer_info['allowed_ips']}\n"
            config += f"Endpoint = {peer_info['endpoint_ip']}:{peer_info['endpoint_port']}\n"
            config += f"PersistentKeepalive = {self._keepalive}\n\n"
        
        return config


    def ping_all_peers(self)-> None:
        """ Pings all peers to check connectivity. Used for debugging and testing. """
        for peer_ip in self.peers.keys():
            print(f"[STATE] Pinging peer {peer_ip}")
            ping(peer_ip, verbose=True, count=2, timeout=0.5)

    
    def disableNetlink(self):
        """ Disables the WireGuard interface using netlink. Used when exiting the program to clean up the interface. """
        idx = self.ipr.link_lookup(ifname=self.interface)[0]
        self.ipr.link("delete", index=idx)

        self.ipr.close()
        self.wg.close()

    def netlinkUp(self):
        """ Brings the WireGuard interface up using netlink. """
        idx = self.ipr.link_lookup(ifname=self.interface)[0]
        self.ipr.link("set", index=idx, state="up")

    def netlinkDown(self):
        """ Brings the WireGuard interface down using netlink. Only brings it down, does not delete it.  """
        idx = self.ipr.link_lookup(ifname=self.interface)[0]
        self.ipr.link("set", index=idx, state="down")

    def interface_json(self)-> dict:
        """ Returns information about the interface. Used for discovery purposes. """
        return {
            "ip": self.ip,
            "port": self.public_port,
            "public_key": self.public_key,
            "public_ip": self.public_ip
        }
        