from time import sleep

from .base import DiscoveryBase

from zeroconf import IPVersion, ServiceInfo, ServiceStateChange, Zeroconf, ServiceBrowser, ZeroconfServiceTypes
from .broadcast import DiscoveryBroadcast
from .join import DiscoveryJoin
import socket
from state import State

TYPE_JOIN = "JOIN"
TYPE_BROADCAST = "BROADCAST"
KEY_TYPE = "type"
KEY_IP = "ip"
KEY_PORT = "port"

class DiscoveryDNSSD():
    def __init__(self, injected_state: State| None = None) -> None:
        self.state = injected_state
        self.zeroconf = Zeroconf(ip_version=IPVersion.V4Only)
        self.join_service_info = None
        self.broadcast_service_info = None
        self.available_services = []

    def startAdvertise(self, discovery_instance: DiscoveryJoin | DiscoveryBroadcast) -> None:
        print("[DNSSD] Starting DNSSD discovery...")
        
        if discovery_instance:
            if isinstance(discovery_instance, DiscoveryJoin):
                self._registerJOINService(discovery_instance.getInfo())
            elif isinstance(discovery_instance, DiscoveryBroadcast):
                self._registerBroadcastService(discovery_instance.getInfo())

    def _registerJOINService(self, info: dict) -> None:
        address = input("Input reachable IP address for direct join:\n(default: 127.0.0.1)").strip()
        if not address:
            address = "127.0.0.1"
        
        address = socket.gethostbyname(address)

        port = info["port"]

        self.join_service_info = ServiceInfo(
            "_wg._tcp.local.",
            f"{self.state.ip}_join._wg._tcp.local.",
            addresses=[socket.inet_aton(address)],
            port=port,
            properties={
                KEY_TYPE: info["type"],
                KEY_IP: address,
                KEY_PORT: str(port)
                }
        )
        print(f"[DNSSD] Registering JOIN service with address {address}:{port}...")
        self.zeroconf.register_service(self.join_service_info)

    def _registerBroadcastService(self, info: dict) -> None:
            port = info["port"]

            self.broadcast_service_info = ServiceInfo(
                "_wg._tcp.local.",
                f"{self.state.ip}_broadcast._wg._tcp.local.",
                addresses=[socket.inet_aton("0.0.0.0")],
                port=port,
                properties={
                    KEY_TYPE: info["type"],
                    KEY_IP: "0.0.0.0",
                    KEY_PORT: str(port)
                }
            )
            print(f"[DNSSD] Registering BROADCAST service with port {port}...")
            self.zeroconf.register_service(self.broadcast_service_info)
            

    def stopAdvertise(self) -> None:
        print("[DNSSD] Stopping DNSSD discovery...")
        if self.join_service_info:
            self.zeroconf.unregister_service(self.join_service_info)
        if self.broadcast_service_info:
            self.zeroconf.unregister_service(self.broadcast_service_info)
        if self.zeroconf:
            self.zeroconf.close()

    def browseServices(self) -> None:
        print(f"[DNSSD] Scanning for services on the local network...")
        services = [
            "_wg._tcp.local."
        ]
        self.browser = ServiceBrowser(self.zeroconf, services, handlers=[self.on_service_state_change])
        while True:
            selected_service = input("Select **index** of service to join:\n").strip()
            if not selected_service.isdigit():
                print("Please enter a valid index number.")
                sleep(1)
                continue
            selected_service = int(selected_service)
            # if selected_service in self.available_services.keys():
            if selected_service < len(self.available_services):
                info = self.available_services[selected_service]["info"]
                name = self.available_services[selected_service]["name"]
                # info = self.available_services[selected_service]
                print(f"[DNSSD] Selected service **{selected_service}** ({name}).")
                self.zeroconf.close()
                ret = {
                    "type" : info.properties.get(KEY_TYPE.encode()).decode('utf-8'),
                    "ip": info.properties.get(KEY_IP.encode()).decode('utf-8'),
                    "port": int(info.properties.get(KEY_PORT.encode()).decode('utf-8'))
                }
                return ret
            else:
                print(f"Invalid selection.")
                


    def on_service_state_change(self, zeroconf: Zeroconf, service_type: str, name: str, state_change: ServiceStateChange) -> None:
        
        if state_change is ServiceStateChange.Added:
            info = zeroconf.get_service_info(service_type, name)
            if info:
                # self.available_services[name] = info
                self.available_services.append({"name": name, "info": info})
                print(f"[DNSSD] **{self.available_services.index({'name': name, 'info': info})}** Resolved service {name}:\n {info}\n")
            else:
                print(f"[DNSSD] Failed to resolve service {name}")
        elif state_change is ServiceStateChange.Removed:
            print(f"[DNSSD] Service {name} removed")
            if info in self.available_services:
                # del self.available_services[name]
                self.available_services.remove(info)
        elif state_change is ServiceStateChange.Updated:
            info = zeroconf.get_service_info(service_type, name)
            if info:
                # self.available_services[name] = info
                for idx, service in enumerate(self.available_services):
                    if service["name"] == name:
                        self.available_services[idx]["info"] = info
                        break
                
                print(f"[DNSSD] **{self.available_services.index({'name': name, 'info': info})}** Service {name} updated:\n {info}\n")