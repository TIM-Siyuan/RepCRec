from src.managers import DataManager
from src.managers import LockManager
from src.models.enum import SiteStatus
from src.models.enum import LockType


class Site(object):

    def __init__(self, site_id):
        self.site_id = site_id
        self.data_manager = DataManager(site_id)
        self.lock_manager = LockManager()
        self.STATUS = SiteStatus.UP

    def dump(self):
        self.data_manager.dump()

    def isUP(self):
        return SiteStatus == SiteStatus.UP

    def getDataManager(self):
        return self.data_manager

    def getLockManager(self):
        return self.lock_manager

    def commit(self, tid, time, updatedV):
        writtenvalues = {}
        for vid, value in updatedV.items():
            if self.lock_manager.isHoldingLock(LockType.WRITE, vid, tid):
                writtenvalues[vid] = updatedV[vid]
        self.data_manager.commitVariables(time, writtenvalues)
        self.lock_manager.releaseAllLocks(tid)

    def abort(self, tid):
        self.lock_manager.releaseAllLocks(tid)

    def fail(self):
        self.STATUS = SiteStatus.DOWN
        self.lock_manager.clear()
        self.data_manager.fail()

    def recover(self):
        self.STATUS = SiteStatus.UP
        self.data_manager.recover()
