from src.models.enum import DataType
from src.models.DataCopy import DataCopy


class DataManager:
    def __init__(self, site_id):
        self.site_id = site_id
        self.datacopies = {}
        for i in range(20):
            if i % 2 == 0:
                self.datacopies[i] = DataCopy(DataType.REPLICATED, i)
            if i % 2 == 1 and i % 10 + 1 == self.site_id:
                self.datacopies[i] = DataCopy(DataType.NONREPLICATED, i)

    def isAvailable(self, vid):
        return (self.datacopies.get(vid)).read_available

    def read(self, vid):
        return (self.datacopies.get(vid)).getLastestCommit()

    def recover(self):
        for key, value in self.datacopies.items():
           if value.getDataType() == DataType.NONREPLICATED:
               value.setReadAvailable(True)

    def fail(self):
        for key, value in self.datacopies.items():
            value.setReadAvailable(False)

    def commit(self, time, new_vids):
        for key, value in new_vids.item():
            self.datacopies.get(key).addCommitHistory(time, value)
            self.datacopies.get(key).setReadAvailable(True)

    #getSnapshot
    def getSnapshot(self, vid, timestamp):
        datacopy = self.datacopies.get(vid);
        commits = datacopy.getCommitHistory()
        for commit in commits:
            if commit[0] < timestamp:
                snapshot = commit
        return snapshot

    #dump
    def dump(self):
        #dump site
        return None





