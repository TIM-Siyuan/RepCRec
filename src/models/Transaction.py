from src.models.enum import LockType, TransactionType
from src.models.enum import TransactionStatus


class Transaction:

    def __init__(self, tid, begin_time, trans_type, trans_status, avail_sites, holding_locks, local_cache):
        self.tid = tid
        self.begin_time = begin_time
        self.trans_type = trans_type
        self.trans_status = trans_status
        self.avail_sites = avail_sites
        self.holding_locks = holding_locks
        self.local_cache = local_cache

    def addAvailableSite(self, first_avail_time, site_id):
        if site_id not in self.avail_sites:
            self.avail_sites[site_id] = first_avail_time

    def isHoldingLock(self, vid, lock_type):
        if vid not in self.holding_locks:
            return False

        if lock_type == self.holding_locks[vid]:
            return True

        return lock_type == LockType.READ

    def isReadOnly(self):
        return self.trans_type == TransactionType.READ_ONLY

    def getTransType(self):
        return self.trans_type

    def getBeginTime(self):
        return self.begin_time

    def getAvailSites(self):
        return self.avail_sites

    def addLock(self, vid, lock_type):
        if vid not in self.holding_locks:
            self.holding_locks[vid] = lock_type

    def readLoclCache(self, vid):
        return self.local_cache[vid]

    def write(self, vid, val):
        self.local_cache[vid] = val

    def setStatus(self, trans_status):
        self.trans_status = trans_status

    def getStatus(self):
        return self.trans_status

    def getCache(self):
        return self.local_cache
