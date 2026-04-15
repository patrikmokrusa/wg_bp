from .base import SyncBase
from .dht import SyncDHT
from .gossip import SyncGossip
from .mq import MessageQueueSync
from state import State


class AllSync(SyncBase):
    """
    Helper class to combine multiple synchronization mechanisms into one.
    """
    def __init__(self, injected_state : State, sync_list: list[SyncDHT | SyncGossip | MessageQueueSync]):
        """ Constructor for the AllSync class. Takes a list of synchronization modules to combine. """
        self.state = injected_state
        """ The state object to synchronize. """
        self.sync_list = sync_list
        """ List of synchronization modules to combine. """

    def getInfo(self) -> dict:
        """ Returns information about the synchronization. Used for discovery. """
        sub_sync_info = []
        for sync in self.sync_list:
            sub_sync_info.append(sync.getInfo())
        ret = {
            "sync-type": "ALL",
            "sync-list": sub_sync_info
        }
        return ret
    
    def splitInfo(info: dict) -> tuple:
        """ Helper function to split the information about the synchronization into the individual synchronization modules. """
        sync_list = info.get("sync-list", [])
        dht_info = next((x for x in sync_list if x.get("sync-type") == "DHT"), None)
        gossip_info = next((x for x in sync_list if x.get("sync-type") == "Gossip"), None)
        mq_info = next((x for x in sync_list if x.get("sync-type") == "MQ"), None)
        return dht_info, gossip_info, mq_info

    def checkForChanges(self) -> None:
        """This module does not check for changes itself. Its here because of Abstract Base Class. """
        pass
    
    def publishChange(self, virtual_ip: str, public_key: str, endpoint_ip: str, endpoint_port: int, sync_port: int | None=None) -> None:
        """ Publishes a change to all synchronization modules in the list. """
        for sync in self.sync_list:
            sync.publishChange(virtual_ip, public_key, endpoint_ip, endpoint_port, sync_port)

    def exitSync(self) -> None:
        """ Exits and cleans up all synchronization modules in the list. """
        for sync in self.sync_list:
            sync.exitSync()
        