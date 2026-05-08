# Autor: Patrik Mokruša (xmokrup00)
import abc
from state import State

class DiscoveryBase(abc.ABC):
    """ Base class for discovery modules. Defines the interface for discovery modules. """

    @abc.abstractmethod
    def stopAccept(self):
        """ Stops the accept loop. """
        pass

    @abc.abstractmethod
    def startAccept(self):
        """ Starts the accept loop for accepting connections. """
        pass

    @abc.abstractmethod
    def startJoin(self, bootstrap_node: str):
        """ Starts the join process by connecting to a node. """
        pass

    @abc.abstractmethod
    def getInfo(self):
        """ Gets information about the discovery module. """
        pass
    