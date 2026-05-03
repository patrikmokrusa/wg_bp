# Autor: Patrik Mokruša (xmokrup00)
from time import sleep

from .base import DiscoveryBase

from zeroconf import IPVersion, ServiceInfo, ServiceStateChange, Zeroconf, ServiceBrowser, ZeroconfServiceTypes
from .broadcast import DiscoveryBroadcast
from .join import DiscoveryJoin
import socket
from state import State

# TYPE_JOIN = "JOIN"

# TYPE_BROADCAST = "BROADCAST"
KEY_TYPE = "type"
KEY_IP = "ip"
KEY_PORT = "port"

class DiscoveryDNSSD():
    """ 
    Discovery module using DNS Service Discovery (DNSSD) to advertise and discover services on the local network. 
    Currently supports advertising JOIN and BROADCAST services.
    """
    def __init__(self, injected_state: State| None = None) -> None:
        """ Constructor for the DiscoveryDNSSD class. Initializes the state and Zeroconf instance. """
        self._state = injected_state
        self._zeroconf = Zeroconf(ip_version=IPVersion.V4Only)
        self._join_service_info = None
        self._broadcast_service_info = None
        self._available_services = []

    def startAdvertise(self, discovery_instance: DiscoveryJoin | DiscoveryBroadcast) -> None:
        """ Starts advertising the services on the local network based on the provided discovery instance."""
        print("[DNSSD] Starting DNSSD discovery...")
        
        if discovery_instance:
            if isinstance(discovery_instance, DiscoveryJoin):
                self._registerJOINService(discovery_instance.getInfo())
            elif isinstance(discovery_instance, DiscoveryBroadcast):
                self._registerBroadcastService(discovery_instance.getInfo())

    def _registerJOINService(self, info: dict) -> None:
        """ Registers the JOIN service on the local network with the provided information. """
        address = input("Input reachable IP address for direct join:\n(default: 127.0.0.1)").strip()
        if not address:
            address = "127.0.0.1"
        
        address = socket.gethostbyname(address)

        port = info["port"]

        self._join_service_info = ServiceInfo(
            "_wg._tcp.local.",
            f"{self._state.ip}_join._wg._tcp.local.",
            addresses=[socket.inet_aton(address)],
            port=port,
            properties={
                KEY_TYPE: info["type"],
                KEY_IP: address,
                KEY_PORT: str(port)
                }
        )
        print(f"[DNSSD] Registering JOIN service with address {address}:{port}...")
        self._zeroconf.register_service(self._join_service_info)

    def _registerBroadcastService(self, info: dict) -> None:
        """ Registers the BROADCAST service on the local network with the provided information. """
        port = info["port"]

        self._broadcast_service_info = ServiceInfo(
            "_wg._tcp.local.",
            f"{self._state.ip}_broadcast._wg._tcp.local.",
            addresses=[socket.inet_aton("0.0.0.0")],
            port=port,
            properties={
                KEY_TYPE: info["type"],
                KEY_IP: "0.0.0.0",
                KEY_PORT: str(port)
            }
        )
        print(f"[DNSSD] Registering BROADCAST service with port {port}...")
        self._zeroconf.register_service(self._broadcast_service_info)
            

    def stopAdvertise(self) -> None:
        """ Stops active advertising of services and unregisters them from the local network. """
        print("[DNSSD] Stopping DNSSD discovery...")
        if self._join_service_info:
            self._zeroconf.unregister_service(self._join_service_info)
        if self._broadcast_service_info:
            self._zeroconf.unregister_service(self._broadcast_service_info)
        if self._zeroconf:
            self._zeroconf.close()

    def browseServices(self) -> None:
        """ Browses for available services on the local network and allows the user to select one to join. """
        print(f"[DNSSD] Scanning for services on the local network...")
        services = [
            "_wg._tcp.local."
        ]
        self.browser = ServiceBrowser(self._zeroconf, services, handlers=[self._on_service_state_change])
        while True:
            selected_service = input("Select **index** of service to join:\n").strip()
            if not selected_service.isdigit():
                print("Please enter a valid index number.")
                sleep(1)
                continue
            selected_service = int(selected_service)

            if selected_service < len(self._available_services):
                info = self._available_services[selected_service]["info"]
                name = self._available_services[selected_service]["name"]

                print(f"[DNSSD] Selected service **{selected_service}** ({name}).")
                self._zeroconf.close()
                ret = {
                    "type" : info.properties.get(KEY_TYPE.encode()).decode('utf-8'),
                    "ip": info.properties.get(KEY_IP.encode()).decode('utf-8'),
                    "port": int(info.properties.get(KEY_PORT.encode()).decode('utf-8'))
                }
                return ret
            else:
                print(f"Invalid selection.")
                


    def _on_service_state_change(self, zeroconf: Zeroconf, service_type: str, name: str, state_change: ServiceStateChange) -> None:
        """ Callback function to handle changes in the state of services on the local network."""
        if state_change is ServiceStateChange.Added:
            info = zeroconf.get_service_info(service_type, name)
            if info:
                self._available_services.append({"name": name, "info": info})
                print(f"[DNSSD] **{self._available_services.index({'name': name, 'info': info})}** Resolved service {name}:\n {info}\n")
            else:
                print(f"[DNSSD] Failed to resolve service {name}")
        elif state_change is ServiceStateChange.Removed:
            print(f"[DNSSD] Service {name} removed")
            if info in self._available_services:
                self._available_services.remove(info)
        elif state_change is ServiceStateChange.Updated:
            info = zeroconf.get_service_info(service_type, name)
            if info:
                for idx, service in enumerate(self._available_services):
                    if service["name"] == name:
                        self._available_services[idx]["info"] = info
                        break
                
                print(f"[DNSSD] **{self._available_services.index({'name': name, 'info': info})}** Service {name} updated:\n {info}\n")