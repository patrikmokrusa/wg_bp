from .base import SyncBase
from .dht import SyncDHT
from .gossip import SyncGossip
from .mq import MessageQueueSync
from state import State


class AllSync(SyncBase):
    def __init__(self, injected_state : State, sync_list: list[SyncDHT | SyncGossip | MessageQueueSync]):
        self.state = injected_state
        self.sync_list = sync_list

    def getInfo(self) -> dict:
        sub_sync_info = []
        for sync in self.sync_list:
            sub_sync_info.append(sync.getInfo())
        ret = {
            "sync-type": "ALL",
            "sync-list": sub_sync_info
        }
        return ret
    
    def splitInfo(info: dict) -> tuple:
        sync_list = info.get("sync-list", [])
        dht_info = next((x for x in sync_list if x.get("sync-type") == "DHT"), None)
        gossip_info = next((x for x in sync_list if x.get("sync-type") == "Gossip"), None)
        mq_info = next((x for x in sync_list if x.get("sync-type") == "MQ"), None)
        return dht_info, gossip_info, mq_info

    def checkForChanges(self) -> None:
        """This module does not check for changes itself"""
        pass
    
    def publishChange(self, virtual_ip: str, public_key: str, endpoint_ip: str, endpoint_port: int, sync_port: int | None=None) -> None:
        for sync in self.sync_list:
            sync.publishChange(virtual_ip, public_key, endpoint_ip, endpoint_port, sync_port)

    def exitSync(self) -> None:
        for sync in self.sync_list:
            sync.exitSync()
        