from src.models.enum import LockType


class LockManager(object):

    def __init__(self):
        self.lock_tables = {}

    def acquireLock(self, tid, vid, lock_type):
        lock_list = self.lock_tables[vid]
        conflict_list = []
        # try to acquire read lock
        if lock_type == LockType.READ:
            for lock in lock_list:
                if lock[1] == LockType.WRITE:
                    conflict_list.append(lock[0])
                    return conflict_list

            # acquire read lock successfully
            self.addLock(lock_type, tid, vid)
            return

        # if acquire write lock successfully
        if len(lock_list) == 0 or (len(lock_list) == 1 and (tid in lock_list.keys())):
            self.addLock(lock_type, tid, vid)
            return

        # acquire write lock failed
        for lock in lock_list:
            conflict_list.append(lock[0])

        return conflict_list


    def releaseWriteLock(self, tid, vid, is_holding_read_lock):
        lock_list = self.lock_tables[vid]
        if tid in lock_list:
            lock_list.pop(tid)

        self.lock_tables[vid] = lock_list
        if is_holding_read_lock:
            self.addLock(LockType.READ, tid, vid)


    def releaseAllLocks(self, tid):
        for vid in self.lock_tables.keys():
            lock_list = self.lock_tables[vid]
            lock_list.pop(tid)

    def clear(self):
        self.lock_tables.clear()

    def addLock(self, tid, vid, lock_type):
        lock_list = self.lock_tables[vid]
        if tid not in lock_list.keys or lock_type == LockType.WRITE:
            lock_list[tid] = lock_type
        self.lock_tables[vid] = lock_list

    def isHoldingLock(self, tid, vid, lock_type):
        if vid not in self.lock_tables:
            return False
        lock_list = self.lock_tables[vid]

        if tid not in self.lock_tables:
            return False

        if lock_list[tid] == LockType.READ and lock_type == LockType.WRITE
            return False

        return True