from src.managers import DataManager
from src.managers import LockManager
from src.models.enum import site_status
from src.models.enum import SiteStatus



class Site(object):

    def __init__(self, site_id):
        self.site_id = site_id
        self.data_manager = DataManager(site_id)
        self.lock_manager = LockManager()
        self.STATUS = site_status.UP


    def dump(self):
        self.data_manager.dump()


    def isUP(self):
        return site_status == site_status.UP


    def getDataManager(self):
        return self.data_manager


    def getLockManager(self):
        return self.lock_manager


    def commit(self, tid, time, updatedV):
        writtenvalues = {}
        for key, value in updatedV.items():
            if self.lock_manager.isHoldingLock(LockType)


