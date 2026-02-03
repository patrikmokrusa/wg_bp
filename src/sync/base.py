import abc


class SyncBase(abc.ABC):

    @abc.abstractmethod
    def initSync(self):
        pass

    @abc.abstractmethod
    def publishChange(self, virtual_ip, public_key, endpoint_ip, endpoint_port):
        pass

    @abc.abstractmethod
    def checkForChanges(self):
        pass

    @abc.abstractmethod
    def getInfo(self):
        pass

    @abc.abstractmethod
    def exitSync(self):
        pass
    