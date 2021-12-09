from src.CustomizedConf import LockType


class LockManager:
    """
    A class to manager lock
    """

    def __init__(self):
        self.lock_table = {}

    def acquire_read_lock(self, tid, vid):
        """
        Try to acquire read lock in a variable

        :param vid: variable id
        :param tid: transaction id
        :return: Boolean
        """
        if vid not in self.lock_table:
            # add transaction_id to the read lock list
            self.lock_table[vid] = {LockType.READ: {tid}, LockType.WRITE: None}
            return True
        else:
            # no write lock or own write lock by itself
            if self.lock_table[vid][LockType.WRITE] is None or self.lock_table[vid][LockType.WRITE] == tid:
                self.lock_table[vid][LockType.READ].add(tid)
                return True
            # has other write lock, refused to acquire the read lock
            elif self.lock_table[vid][LockType.WRITE] is not None:
                return False

    def acquire_write_lock(self, tid, vid):
        """
        Try to acquire write lock in a variable

        :param vid: variable id
        :param tid: transaction id
        :return: Boolean
        """
        if vid not in self.lock_table:
            # add transaction_id to write lock list
            self.lock_table[vid] = {LockType.READ: set(), LockType.WRITE: tid}
            return True
        else:
            # promote existed read lock
            if tid in self.lock_table[vid][LockType.READ] and len(self.lock_table[vid][LockType.READ]) == 1:
                self.lock_table[vid][LockType.READ].remove(tid)
                self.lock_table[vid][LockType.WRITE] = tid
                return True
            # has already acquired write lock
            elif tid == self.lock_table[vid][LockType.WRITE]:
                return True
            else:
                return False

    def release_read_lock(self, tid, vid):
        """
        Try to release read lock in a variable

        :param vid: variable id
        :param tid: transaction id
        :return: None
        """
        if tid in self.lock_table[vid][LockType.READ]:
            self.lock_table[vid][LockType.READ].remove(tid)

    def release_write_lock(self, tid, vid):
        """
        Try to release write lock in a variable

        :param vid: variable id
        :param tid: transaction id
        :return: None
        """
        if tid == self.lock_table[vid][LockType.WRITE]:
            self.lock_table[vid][LockType.WRITE] = None

    def release_lock(self, tid, vid):
        self.release_read_lock(tid, vid)
        self.release_write_lock(tid, vid)

    def release_locks_by_trans(self, tid):
        """
        Try to release all locks set by tid

        :param tid: Transaction id
        :return: None
        """
        variable_delete_list = []
        for vid, lock_list in self.lock_table.items():
            # release read lock
            self.release_read_lock(tid, vid)

            # release write lock
            self.release_write_lock(tid, vid)

            if len(lock_list[LockType.READ]) == 0 and lock_list[LockType.WRITE] is None:
                variable_delete_list.append(vid)

        for vid in variable_delete_list:
            self.lock_table.pop(vid)

    def get_all_transactions(self):
        """
        Get all the transactions in this site

        :return: set
        """
        transactions = set()
        for vid, lock_list in self.lock_table.items():
            # Read Transaction on vid
            for tid in lock_list[LockType.READ]:
                transactions.add(tid)
            # Write transaction on vid:
            if lock_list[LockType.WRITE]:
                transactions.add(lock_list[LockType.WRITE])
        return transactions

    def fail(self):
        """
        Clear the lock table after site failed

        :return: None
        """
        self.lock_table = {}
