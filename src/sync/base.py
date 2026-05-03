# Autor: Patrik Mokruša (xmokrup00)
import abc


class SyncBase(abc.ABC):

    @abc.abstractmethod
    def publishChange(self, virtual_ip, public_key, endpoint_ip, endpoint_port, allowed_ips, sync_port=None):
        pass

    @abc.abstractmethod
    def checkForChanges(self):
        """Check for changes in the synchronization mechanism against state and update it accordingly.
        This method should be called periodically"""
        pass

    @abc.abstractmethod
    def getInfo(self):
        pass

    @abc.abstractmethod
    def exitSync(self):
        pass


    