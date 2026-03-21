import abc


class SyncBase(abc.ABC):

    @abc.abstractmethod
    def initSync(self):
        pass

    @abc.abstractmethod
    def publishChange(self, virtual_ip, public_key, endpoint_ip, endpoint_port, sync_port=None):
        pass

    # @abc.abstractmethod
    # def checkForChanges(self):
    #     """Check for changes in the synchronization mechanism against state and update it accordingly.
    #     This method should be called periodically"""
    #     pass

    @abc.abstractmethod
    def getInfo(self):
        pass

    @abc.abstractmethod
    def exitSync(self):
        pass


    def check_individual_peer_change(self, peer_info, existing_peer):
        if (peer_info["public_key"] != existing_peer["public_key"] or
                peer_info["endpoint_ip"] != existing_peer["endpoint_ip"] or
                peer_info["endpoint_port"] != existing_peer["endpoint_port"]):
                self.state.remove_peer(peer_info["virtual_ip"])
                self.state.add_peer(
                    peer_info["virtual_ip"],
                    peer_info["public_key"],
                    peer_info["endpoint_ip"],
                    peer_info["endpoint_port"]
                )
                print(f"""
                      Updated via sync:
                      Before:
                      {peer_info['virtual_ip']} -> {existing_peer}
                      After:
                      {peer_info['virtual_ip']} -> {peer_info}
                      """)
                return True
        return False
    