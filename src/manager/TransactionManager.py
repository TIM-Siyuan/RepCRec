from src.CustomizedConf import *
from src.DeadLockDetector import *
from src.model.Site import Site
from src.model.Transaction import Transaction
from src.utils import IOUtils
from src.Exception import *
from src.model.Operation import Operation

class TransactionManager:
    """
    The transaction manager distributes operation and hold the information of the entire simulation

    :param self.transactions: A dict to store running transactions
    :param self.wait_for_graph: A graph store the waiting sequence of operations to detect deadlock
    :param self.waiting_list: A waiting list contains all blocked operations
    :param self.waiting_trans: A waiting set store all waiting transactions
    :param self.sites: A list store all sites
    """

    def __init__(self):
        self.transactions = {}
        self.waiting_list = []
        self.waiting_trans = set()
        self.wait_for_graph = DeadLockDetector()
        self.sites = [Site(i) for i in range(1, num_sites + 1)]

    def execute_operation(self, operation):
        """
        Process the new operation

        :param operation: new operation
        :param time_stamp:
        :return: None
        """

        # retry
        self.retry()
        is_succeed = self.assign_task(operation)
        if not is_succeed:
            self.waiting_list.append(operation)

        # detect deadlock and then abort the youngest transaction if deadlock happens
        if (OperationType.READ in operation
                or OperationType.WRITE in operation) \
                and self.wait_for_graph.check_deadlock():
            trans = self.find_youngest_trans(self.wait_for_graph.get_trace())[0]
            self.abort(trans, AbortType.DEADLOCK)

    def retry(self):
        """
        retry blocked operations

        :return: None
        """
        blocked_list = []
        blocked_trans = set()
        for op in self.waiting_list:
            if not assign_task(op, True):
                blocked_list.append(op)
                if op.get_type() == OperationType.END and op.get_tid() not in blocked_trans:
                    continue
                blocked_trans.add(op.get_tid())

        self.waiting_list = blocked_list
        self.waiting_trans = blocked_trans

    def find_youngest_trans(self, trace):
        """
        Find the youngest transaction

        :param trace: The deadlock cycle
        :return: The youngest transaction
        """
        ages = [(tid, self.transactions[tid].time_stamp) for tid in trace]
        return sorted(ages, key=lambda x: -x[1])[0]

    def abort(self, tid, abort_type):
        """
        Abort the transaction
        # Steps:
        #   1. release locks
        #   2. revert transaction changes
        #   3. delete transaction in TM
        :param tid: transaction_id
        :param abort_type: abort reason
        :return: None
        """
        for site in self.sites:
            if site.get_status() == SiteStatus.UP:
                site.lock_manager.release_locks_by_trans(tid)
                site.data_manager.revert_trans_changes(tid)

        # Remove any blocked operation belongs to this transaction
        self.waiting_list = [op for op in self.waiting_list if op.get_parameters()[0] != tid]

        # Remove transaction in wait for graph
        self.wait_for_graph.remove_transaction(tid)

        self.transactions.pop(tid)
        if abort_type == AbortType.DEADLOCK:
            print(f"Transaction {tid} aborted (deadlock)")
        elif abort_type == AbortType.DEADLOCK:
            print(f"Transaction {tid} aborted (site failure)")
        elif abort_type == AbortType.DEADLOCK:
            print(f"Transaction {tid} aborted (read-only, no data available in sites to read)")
        else:
            raise ValueError(f"Unknown abort type: {abort_type}")

    def assign_task(self, operation, is_retry=False):
        if OperationType.BEGIN == operation.get_type():
            return self.execute_begin(operation)
        elif OperationType.BEGINRO == operation.get_type():
            return self.execute_begin_read_only(operation)
        elif OperationType.WRITE == operation.get_type():
            return self.execute_write(operation, is_retry)
        elif OperationType.READ == operation.get_type():
            return self.execute_read(operation, is_retry)
        elif OperationType.RECOVER == operation.get_type():
            return self.execute_recover(operation)
        elif OperationType.FAIL == operation.get_type():
            return self.execute_fail()
        elif OperationType.DUMP == operation.get_type():
            return self.execute_dump()
        elif OperationType.END == operation.get_type():
            return self.execute_end(operation, is_retry)

    def execute_begin(self, operation):
        """
        Initialize operation for manipulate

        :param operand: specific data from operations.
        :param time_stamp: time
        :return: True or False
        """
        tid = operation.get_tid()
        self.check_operation_validity(tid)

        time_stamp = operation.get_time()
        self.transactions[tid] = Transaction(tid, time_stamp)
        return True

    def execute_begin_read_only(self, operation):
        """
        Initialize the initial operation for read only

        :param operand: specific data from operations.
        :param time_stamp: time
        :return: True or False
        """
        tid = operation.get_tid()
        self.check_operation_validity(tid)

        time_stamp = operation.get_time()
        trans = Transaction(tid, time_stamp)
        trans.set_trans_type(TransactionType.RO)

        self.transactions[tid] = trans

        # take snapshot
        for site in self.sites:
            site.get_snapshot(time_stamp)
        return True

    def execute_read(self, operation, is_retry=False):
        """
        Execute the read operation, for both read and readonly

        :param retry:
        :param operand: specific data from operations.
        :param time_stamp: time
        :return: True or False
        """
        if not is_retry:
            self.save_to_transaction(operation)

        tid, vid = operation.get_tid(), operation.get_vid()
        self.check_operation_validity(tid)

        # Case 1: read_only transaction
        if self.transactions[tid].transaction_type == TransactionType.RO:
            trans_time_stamp = self.transactions[tid].time_stamp
            # 1.1: If xi is not replicated and the site holding xi is up, then the read-only
            # transaction can read it. Because that is the only site that knows about xi.
            if vid % 2 == NumType.ODD:
                site = self.sites[vid % num_sites + 1]
                if site.status == SiteStatus.UP and vid in site.snapshots[trans_time_stamp]:
                    headers = ["Transaction", "Site", "x" + vid]
                    rows = [[tid, f"{site.sid}", f"{site.get_snapshot_variable(trans_time_stamp, vid)}"]]
                    IOUtils.print_result(headers, rows)
                    return True
                else:
                    return False
            # 1.2: If xi is replicated then RO can read xi from site s if xi was committed
            # at s by some transaction T’ before RO began and s was up all the time
            # between the time when xi was commited and RO began. In that case
            # RO can read the version that T’ wrote. If there is no such site then
            # RO can abort.
            else:
                is_data_exist = False
                for site in self.sites:
                    # if the site has the variable is down, has -> True, we could retry latter
                    if site.status == SiteStatus.DOWN \
                            and trans_time_stamp in site.snapshots \
                            and vid in site.snapshots[trans_time_stamp]:
                        is_data_exist = True
                    elif trans_time_stamp in site.snapshots \
                            and vid in site.snapshots[trans_time_stamp]:
                        headers = ["Transaction", "Site", "x" + vid]
                        rows = [[tid, f"{site.sid}", f"{site.get_snapshot_variable(trans_time_stamp, vid)}"]]
                        IOUtils.print_result(headers, rows)
                        return True
                # No site has the variable in the snapshot
                if not is_data_exist:
                    self.abort(tid, AbortType.NO_DATA_FOR_READ_ONLY)
                    return True
        # Case 2: read-write transaction
        else:
            # 2.1 check specific site the index of variable read is odd (non-replicated)
            for site in self.generate_site_list(vid):
                if site.status == SiteStatus.UP \
                        and site.data_manager.is_available(vid) \
                        and site.lock_manager.acquire_read_lock(tid, vid):
                    return read_variable(tid, vid, site)
        return False

    def execute_write(self, operation, is_retry=False):
        """
        Execute Write operation

        :param operand: specific data from operations
        :param retry: If the operation is a retry
        :return: True or False
        """
        if not is_retry:
            self.save_to_transaction()

        tid, vid, value = operation.get_tid(), operation.get_vid(), operation.get_value()
        self.check_operation_validity(tid)

        locked_site_list = []
        # Try to lock all sites have given variable
        for site in self.generate_site_list(vid):
            # check site status and append it to locked sites
            if site.status == SiteStatus.UP \
                    and site.lock_manager.acquire_write_lock(tid, vid):
                locked_site_list.append(site)
            # release all locks added previously if it is conflicted
            else:
                for locked_site in locked_site_list:
                    locked_site.lock_manager.release_lock(tid, vid)
                return False

        # retry later if not available list now
        if len(locked_site_list) == 0:
            return False

        # execute write operation
        for locked_site in locked_site_list:
            logs = locked_site.data_manager.uncommitted_log.get(tid, {})
            logs[vid] = value
            locked_site.data_manager.uncommitted_log[tid] = logs

        return True

    def execute_dump(self):
        TABLE_HEADERS = ["Site Name"] + [f"x{i}" for i in range(1, num_distinct_variables + 1)]
        rows = [site.print_all_sites() for site in self.sites]
        IOUtils.print_result(TABLE_HEADERS, rows)
        return True

    def execute_fail(self, operation):
        # get site_id
        site = self.sites[operation.get_sid()]
        # get all trans in this site
        trans = site.lock_manager.get_all_transactions()
        for tid in trans:
            self.transactions[tid].set_trans_status(TransactionStatus.ABORTED)
        site.fail()
        return True

    def execute_recover(self, operation):
        site = self.sites[operation.get_sid()]
        site.recover()
        return True

    def execute_end(self, operation, is_retry=False):
        if not is_retry:
            self.save_to_transaction()

        tid = operation.get_tid()
        self.check_operation_validity(tid)
        trans_start_time = self.transactions[tid].time_stamp

        # Two situation need to abort commit operation

        # 1. If a transaction T accesses an item (really accesses it, not just request
        # a lock) at a site and the site then fails, then T should continue to execute
        # and then abort only at its commit time (unless T is aborted earlier due to
        # deadlock).
        if self.transactions[tid].transaction_status == TransactionStatus.ABORTED:
            self.abort(tid, AbortType.SITE_FAILURE)
            return True

        # 2. If there are blocked operation of the commit transaction, block the commit
        if tid in self.waiting_list:
            return False

        # execute commit operation
        for site in self.sites:
            if site.status == SiteStatus.UP:
                if tid in site.data_manager.uncommitted_log:
                    logs = site.data_manager.uncommitted_log[tid]
                    for vid, val in logs.items():
                        site.data_manager.set_variable(vid, val)
                        site.data_manager.set_available(vid, True)
                    # committed, delete it from uncommitted_log
                    site.data_manager.uncommitted_log.pop(tid)
                # when readonly transaction end, delete the snapshot belongs to it
                elif trans_start_time in site.snapshots:
                    site.snapshots.pop(trans_start_time)
            # release all locks by this committed transaction
            site.lock_manager.release_locks_by_trans(tid)

        if tid in self.waiting_trans:
            self.transactions.pop(tid)
        # When transaction commit, we need to remove the transaction in the wait for graph
        self.wait_for_graph.remove_transaction(tid)

        IOUtils.print_commit_result(tid)

        return True

    def check_operation_validity(self, tid):
        if not self.transactions.get(tid):
            raise InvalidOperationError("Transaction {} does not exist".format(tid))

    def generate_site_list(self, vid):
        """
        check vid status and return site list
        :param vid: variable id
        :return: List
        """
        site_list = []

        if vid % 2 == NumType.ODD:
            # non-replicated var find the only site
            s = self.sites[vid % num_sites + 1]
        else:
            s = self.sites

        if len(s) > 1:
            site_list.extend(self.sites)
        else:
            site_list.append(self.sites)

        return site_list

    def save_to_transaction(self, operation):
        """
        Append operation to corresponding transaction's operation list
        :param tm: Transaction Manager
        :return: None
        """
        tid = operation.get_tid()
        # if tid == None:
        #     raise TypeError("Try to append dump operation to transaction")

        if tid not in self.transactions:
            raise KeyError(f"Try to execute {operation.get_type()} in a non-existing transaction")

        self.transactions[tid].add_operation(self)
        self.wait_for_graph.add_operation(self)

    # Read variable from log if the variable was modified by transaction,
    # otherwise read from committed data
    def read_variable(self, tid, vid, sid):
        """
        Read the variable, and print in prettytable

        :param sid: site id
        :param tid: trans id
        :param vid: variable id
        :return:
        """

        # Case 1: data has been changed by the same transaction but not commit before
        if tid in site.data_manager.log and vid in site.data_manager.log[tid]:
            res = site.data_manager.uncommitted_log[tid][vid]
        else:
            res = site.data_manager.get_variable(vid)

        print_result(["Transaction", "Site", f"x{vid}"], [[tid, f"{site.sid}", res]])

        return True

