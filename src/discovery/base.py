# Autor: Patrik Mokruša (xmokrup00)
import abc
from state import State

class DiscoveryBase(abc.ABC):

    @abc.abstractmethod
    def stopAccept(self):
        pass

    @abc.abstractmethod
    def startAccept(self):
        pass

    @abc.abstractmethod
    def startJoin(self, bootstrap_node: str):
        pass

    @abc.abstractmethod
    def getInfo(self):
        pass
    