from state import State
from sync.dht import SyncDHT
from threading import Thread
import time

class ChangeChecker:
    def __init__(self, injected_state: State, injected_sync: SyncDHT, interval: int = 20):
        self.state = injected_state
        self.sync = injected_sync
        self.interval = interval
        self.running = True

    def beginWork(self):
        thread = Thread(target=self.run, daemon=True)
        thread.start()

    def run(self):
        
        while self.running:
            time.sleep(self.interval)
            # print("ChangeChecker: Checking for changes...")
            self.sync.checkForChanges()

            # TODO: detect public ip changes

    def forceCheck(self):
        self.sync.checkForChanges()