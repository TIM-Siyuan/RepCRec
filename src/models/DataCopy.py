from src.models.enum import data_type


class DataCopy:
    def __init__(self, type, initialvalue):
        self.read_available = True
        self.type = type
        self.commit_history = {-1: initialvalue}

    def isReadAvailable(self):
        return self.read_available

    def setReadAvailable(self, availability):
        self.read_available = availability

    def getDataType(self):
        return self.type

    def addCommitHistory(self, time, value):
        self.commit_history[time] = value

    def getLastestCommit(self):
        v = max(self.commit_history, key=self.commit_history.get)
        return v

    def getCommitHistory(self):
        return self.commit_history
