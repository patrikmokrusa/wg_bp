from state import State
from sync.dht import SyncDHT
from sync.gossip import SyncGossip
from threading import Thread
import time

class ChangeChecker:
    def __init__(self, injected_state: State, injected_sync, interval: int = 20):
        self.state = injected_state
        self.sync = injected_sync
        self.interval = interval
        self.running = True

    def beginWork(self):
        if isinstance(self.sync, SyncDHT):
            print("Starting ChangeChecker for DHT synchronization...")
            thread = Thread(target=self.run, daemon=True)
            thread.start()
        elif isinstance(self.sync, SyncGossip):
            # handled by internal listener in SyncGossip
            pass

    def run(self):
        
        while self.running:
            # print("ChangeChecker: Checking for changes...")
            self.sync.checkForChanges()
            time.sleep(self.interval)

            # TODO: detect public ip changes

    def forceCheck(self):
        self.sync.checkForChanges()