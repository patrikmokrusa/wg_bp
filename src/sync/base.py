# Autor: Patrik Mokruša (xmokrup00)
import abc


class SyncBase(abc.ABC):
    """ Base class for synchronization modules. Defines the interface for synchronization modules. """

    @abc.abstractmethod
    def publishChange(self, virtual_ip, public_key, endpoint_ip, endpoint_port, allowed_ips, sync_port=None):
        """ Set peer information in the sync. """
        pass

    @abc.abstractmethod
    def checkForChanges(self):
        """Check for changes in the synchronization mechanism against state and update it accordingly.
        This method should be called periodically"""
        pass

    @abc.abstractmethod
    def getInfo(self):
        """Get synchronization information to be sent to other peers during JOIN."""
        pass

    @abc.abstractmethod
    def exitSync(self):
        """Clean up any resources used by the synchronization mechanism before exiting."""
        pass


    