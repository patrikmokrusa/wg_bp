import abc
from state import State

class DiscoveryBase(abc.ABC):

    @abc.abstractmethod
    def startAccept(self):
        pass

    @abc.abstractmethod
    def startJoin(self, bootstrap_node: str):
        pass