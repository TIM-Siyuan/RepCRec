from src.models.enum import data_type
from src.models.DataCopy import DataCopy


class DataManager:
    def __init__(self, site_id):
        self.site_id = site_id
        self.datacopies = {}
        for i in range(20):
            if i % 2 == 0:
                self.datacopies[i] = DataCopy(data_type.REPLICATED, 10*i)
            if i % 2 == 1 and i % 10 + 1 == self.site_id:
                self.datacopies[i] = DataCopy(data_type.NONREPLICATED, 10*i)

    def isAvailable(self, vid):
        return (self.datacopies.get(vid)).read_available

    def read(self, vid):
        return (self.datacopies.get(vid)).getLastestCommit()

    def recover(self):
        for key, value in self.datacopies.items():
            if value.getDataType() == data_type.NONREPLICATED:
                value.setReadAvailable(True)

    def fail(self):
        for key, value in self.datacopies.items():
            value.setReadAvailable(False)

    def commit(self, time, updatedvid):
        for key, value in updatedvid.item():
            self.datacopies.get(key).addCommitHistory(time, value)
            self.datacopies.get(key).setReadAvailable(True)

    # getSnapshot
    # dump
