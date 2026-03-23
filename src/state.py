import subprocess
import urllib
import stun
from pythonping import ping
from pyroute2 import IPRoute, WireGuard
import threading
import asyncio

STUN_SERVER = 'stun1.l.google.com'

class State:
    def __init__(self, ip: str, port: int = 51820, interface="wg0", keepalive=25):
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

    def lock_aquire(self):
        self.lock.acquire()

    def lock_release(self):
        self.lock.release()

    def _iplinkInit(self):
        self.ipr = IPRoute()

        self.ipr.link("add", ifname=self.interface, kind="wireguard")

        idx = self.ipr.link_lookup(ifname=self.interface)[0]

        self.ipr.addr("add", index=idx, address=self.ip, prefixlen=24)

        self.ipr.link("set", index=idx, state="up")

        self._wgInit()
    
    def _wgInit(self):
        self.wg = WireGuard()

        self.wg.set(
            self.interface,
            private_key=self.private_key,
            listen_port=self.port
        )

    def _wg_set(self, interface, **kwargs):
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

    def _wgSetOwnEventLoop(self, interface, **kwargs):
        thread = threading.Thread(target=self._wg_set, args=(interface,), kwargs=kwargs)
        thread.start()
        thread.join()
        

    def update_public_ip_request(self)-> None:
        external_ip = urllib.request.urlopen('https://api.ipify.org').read().decode('utf8')
        self.public_ip = external_ip

    def update_public_ip(self):
    
        print("[STATE] Determining public IP and port via STUN...")
        mapped_addr = stun.get_ip_info('0.0.0.0', self.port, stun_host=STUN_SERVER)
        print(f"[STATE] STUN result: {mapped_addr}")
        if mapped_addr[1] is None or mapped_addr[2] is None:
            print("[STATE] Failed to get public IP via STUN.")
            print("[STATE] Falling back to HTTP request method...")
            self.update_public_ip_request()
            return
        
        
        self.public_ip = mapped_addr[1]
        self.public_port = mapped_addr[2]

    def _gen_private_key(self)-> None:
        cli = subprocess.Popen(["wg", "genkey"], stdout=subprocess.PIPE)
        self.private_key = cli.stdout.read().decode("utf-8")
    
    def _gen_public_key(self)-> None:
        cli = subprocess.Popen(["wg", "pubkey"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        self.public_key = cli.communicate(input=self.private_key.encode("utf-8"))[0].decode("utf-8")

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
        print(f"[STATE] Added peer {peer_virtual_ip}")

    def _getAllowedIPs(self, peer_virtual_ip: str) -> list:
        allowed_ips = []
        if "/" in peer_virtual_ip:
            allowed_ips.append(peer_virtual_ip)
        else:
            allowed_ips.append(peer_virtual_ip + "/24")
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
            print(f"[STATE] Removed peer {peer_virtual_ip}")


    def get_config(self)-> str:
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
            ping(peer_ip, verbose=False, count=10, timeout=0)

    def disable_config(self)-> None:
        return
        subprocess.run(["wg-quick", "down", self.interface])
    
    def disableNetlink(self):
        idx = self.ipr.link_lookup(ifname=self.interface)[0]
        # self.ipr.link("set", index=idx, state="down")
        self.ipr.link("delete", index=idx)

        self.ipr.close()
        self.wg.close()

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
        