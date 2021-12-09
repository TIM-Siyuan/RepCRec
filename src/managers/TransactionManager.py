from src.CustomizedConf import *
from src.DeadLockDetector import *
from src.models.Site import Site
from src.models.Transaction import Transaction
from src.utils import IOUtils
from src.Exception import *


class TransactionManager:
    """
    The transaction manager distributes operation and hold the information of the entire simulation
    """

    def __init__(self):
        self.transactions = {}
        self.operations = []
        self.waiting_list = []
        self.waiting_trans = set()
        self.wait_for_graph = DeadLockDetector()
        self.sites = [Site(i) for i in range(1, num_sites + 1)]

    def two_step_execute(self, operation, time_stamp):
        """
        Process the new operation

        :param operation: new operation
        :param time_stamp:
        :return: None
        """

        # retry
        self.retry(time_stamp)
        is_succeed = self.assign_task(operation, time_stamp)
        if not is_succeed:
            self.blocked.append(operation)

        # detect deadlock and then abort the youngest transaction if deadlock happens
        if (OperationType.READ in operation or OperationType.WRITE in operation) \
                and \
                self.wait_for_graph.check_deadlock():
            trans = self.get_youngest_transaction(self.wait_for_graph.get_trace())[0]
            self.abort(trans, AbortType.DEADLOCK)

    def retry(self, time_stamp):
        """
        retry blocked operations (update blocked operations and blocked transactions)

        :return: None
        """
        op_b = []
        tx_b = set()
        for op in self.waiting_list:
            if not op.execute(time_stamp, self, True):
                op_b.append(op)

                if op.get_op_t() == "end" and op.get_parameters()[0] not in tx_b:
                    continue

                tx_b.add(op.get_parameters()[0])

        self.waiting_list = op_b
        self.waiting_trans = tx_b

    def get_youngest_transaction(self, trace):
        """
        Find the youngest transaction

        :param trace: The deadlock cycle
        :return: The youngest transaction
        """
        ages = [(tid, self.transactions[tid].time_stamp) for tid in trace]
        return sorted(ages, key=lambda x: -x[1])[0]

        # Abort given transaction
        # Steps:
        #   1. release locks
        #   2. revert transaction changes
        #   3. delete transaction in TM

    def abort(self, tid, abort_type):
        """
        Abort the transaction

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

        # Remove transaction in wait graph
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

    def assign_task(self, operation, time_stamp):
        if OperationType.BEGIN in operation:
            operand = operation[OperationType.BEGIN]
            return self.execute_begin(operand, time_stamp)
        elif OperationType.BEGINRO in operation:
            operand = operation[OperationType.BEGINRO]
            return self.execute_begin_read_only(operand, time_stamp)
        elif OperationType.WRITE in operation:
            operand = operation[OperationType.WRITE]
            return self.execute_write(operand, time_stamp)
        elif OperationType.READ in operation:
            operand = operation[OperationType.READ]
            return self.execute_read(operand, time_stamp)
        elif OperationType.RECOVER in operation:
            operand = operation[OperationType.RECOVER]
            return self.execute_recover(operand)
        elif OperationType.FAIL in operation:
            operand = operation[OperationType.FAIL]
            return self.execute_fail(operand)
        elif OperationType.DUMP in operation:
            return self.execute_dump()
        elif OperationType.END in operation:
            operand = operation[OperationType.END]
            return self.execute_end(operand, time_stamp)

    def execute_begin(self, operand, time_stamp):
        """
        Initialize operation for manipulate

        :param operand: specific data from operations.
        :param time_stamp: time
        :return: True or False
        """
        tid = operand[0]
        self.check_operation_validity(tid)
        self.transactions[tid] = Transaction(tid, time_stamp)
        return True

    def execute_begin_read_only(self, operand, time_stamp):
        """
        Initialize the initial operation for read only

        :param operand: specific data from operations.
        :param time_stamp: time
        :return: True or False
        """
        tid = operand[0]
        self.check_operation_validity(tid)

        trans = Transaction(tid, time_stamp)
        trans.set_trans_type(TransactionType.RO)
        self.transactions[tid] = trans

        # take snapshot
        for site in self.sites:
            site.get_snapshot(time_stamp)
        return True

    def execute_read(self, operand, retry=False):
        """
        Execute the read operation, for both read and readonly

        :param retry:
        :param operand: specific data from operations.
        :param time_stamp: time
        :return: True or False
        """
        if not retry:
            self.save_to_transaction()

        tid, vid = operand[0], operand[1][1:]
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

    def execute_write(self, operand, retry=False):
        """
        Execute Write operation

        :param operand: specific data from operations
        :param retry: If the operation is a retry
        :return: True or False
        """
        if not retry:
            self.save_to_transaction()

        tid, vid, value = operand[0], operand[1][1:], operand[2]
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

    def execute_fail(self, operand):
        # get site_id
        site = self.sites[operand[0]]
        # get all trans in this site
        trans = site.lock_manager.get_all_transactions()
        for tid in trans:
            self.transactions[tid].set_trans_status(TransactionStatus.ABORTED)
        site.fail()
        return True

    def execute_recover(self, operand):
        site = self.sites[operand[0]]
        site.recover()
        return True

    def execute_end(self, operand, retry=False):
        if not retry:
            self.save_to_transaction()

        tid = operand[0]
        self.check_operation_validity(tid)
        trans_start_time = self.transactions[tid].time_stamp

        # Two situation need to abort commit operation

        # 1. If a transaction T accesses an item (really accesses it, not just request
        # a lock) at a site and the site then fails, then T should continue to execute
        # and then abort only at its commit time (unless T is aborted earlier due to
        # deadlock).
        if self.transactions[tid].to_be_aborted:
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

    def save_to_transaction(self):
        # TODO save operations
        pass

    def read_variable(self, tid, vid, sid):
        # TODO read variable and print
        pass
